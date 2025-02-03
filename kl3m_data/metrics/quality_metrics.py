"""
Low-level quality metrics to score text quality based on chars and tokens only.
"""

# imports
import argparse
import csv
import math
import string

# packages
import tqdm
from datasets import load_dataset, concatenate_datasets
from huggingface_hub import hf_api
from tokenizers import Tokenizer

DEFAULT_TOKENIZER_NAME = "alea-institute/kl3m-004-128k-cased"
DEFAULT_TOKENIZER = Tokenizer.from_pretrained(DEFAULT_TOKENIZER_NAME)

METRIC_WEIGHTS = {
    "ratio_whitespace": 1.0,
    "average_line_length": 1.0,
    "average_paragraph_length": 1.0,
    "ratio_alphanumeric": 1.0,
    "ratio_alpha_to_numeric": 0.1,
    "ratio_non_ascii": 2.0,
    "ratio_capital": 1.0,
    "ratio_punctuation": 1.0,
    "average_word_length": 1.5,
    "type_token_ratio": 1.5,
    "token_entropy": 0.5,
    "char_entropy": 0.5,
    "max_token_frequency_ratio": 1.0,
    "repetition_rate": 1.5,
    "ratio_format_tokens": 1.0,
    "ratio_nospace_bigrams": 2.0,
}

EXPECTED_RANGES = {
    # based on 2%/98% of usc + cfr text
    "ratio_whitespace": (0.121212, 0.193813),
    "average_line_length": (17.5, 245.0),
    "average_paragraph_length": (35.0, 849.0),
    "ratio_alphanumeric": (0.594595, 0.822884),
    "ratio_alpha_to_numeric": (1.829268, 265.1),
    "ratio_non_ascii": (0.0, 0.034483),
    "ratio_capital": (0.008368, 0.224638),
    "ratio_punctuation": (0.021601, 0.210867),
    "average_word_length": (4.498695, 7.285714),
    "type_token_ratio": (0.387879, 0.66055),
    "repetition_rate": (0.33945, 0.612121),
    "token_entropy": (3.38158, 7.855401),
    "char_entropy": (4.066784, 5.017473),
    "max_token_frequency_ratio": (0.04028, 0.153846),
    # should be zero
    "ratio_format_tokens": (0.0, 0.0),
    "ratio_nospace_bigrams": (0.0, 0.0),
}


# -------------------------------------------------------------------
def score_text(metrics: dict) -> float:
    """
    Given a dictionary 'metrics' with keys corresponding to our metrics,
    compute a soft anomaly score based on deviations from expected ranges.

    The function works as follows:
      - For each metric, if its value is below the expected lower bound,
        we compute a relative deviation: (lower - value) / |lower|.
      - If the value is above the expected upper bound,
        the deviation is: (value - upper) / |upper|.
      - For metrics whose expected range is a fixed value (e.g. 0),
        any nonzero value adds a penalty proportional to its absolute difference.
      - Each deviation is multiplied by a predefined weight.

    Args:
        metrics (dict): A dictionary containing metric names and their values.

    Returns:
        A float representing the overall anomaly score.
    """
    total_score = 0.0
    eps = 1e-8

    for metric, weight in METRIC_WEIGHTS.items():
        if metric not in metrics:
            continue
        value = metrics[metric]
        if metric in EXPECTED_RANGES:
            # expected range is not a single value
            lower, upper = EXPECTED_RANGES[metric]

            # check if the value is -inf, +inf, or nan
            component = 0.0
            if math.isinf(value) or math.isnan(value):
                continue
            else:
                if lower == upper:
                    if abs(value - lower) > eps:
                        component = weight * abs(value - lower)
                else:
                    if value < lower:
                        component = weight * (lower - value) / (abs(lower) + eps)
                    elif value > upper:
                        component = weight * (value - upper) / (abs(upper) + eps)

            # check that component isn't nan or inf
            if math.isnan(component) or math.isinf(component):
                # skip this component
                raise RuntimeError(
                    f"Component for metric {metric} is {component}, metrics are {metrics}, weights are {METRIC_WEIGHTS}"
                )
            total_score += component

    # if the score is nan or inf, raise runtime error and print all metrics
    if math.isnan(total_score) or math.isinf(total_score):
        raise RuntimeError(
            f"Score is {total_score}, metrics are {metrics}, weights are {METRIC_WEIGHTS}"
        )
    return total_score


def get_metrics(record: dict) -> dict:
    """
    Compute and return a dictionary of text-based and token-based metrics
    in as few passes as possible, preserving the same output keys.
    """

    # ----------------------------
    # 1) Decode tokens -> text
    # ----------------------------
    tokens = record.get("tokens", [])
    text = DEFAULT_TOKENIZER.decode(tokens)

    total_chars = len(text)

    # Quick early exit if there's no text at all
    # (We still do token-based checks below)
    if total_chars == 0:
        # Build a minimal skeleton for text metrics
        text_metrics = {
            "total_characters": 0,
            "whitespace_count": 0,
            "ratio_whitespace": 0,
            "num_lines": 0,
            "average_line_length": 0,
            "num_paragraphs": 0,
            "average_paragraph_length": 0,
            "alphanumeric_count": 0,
            "ratio_alphanumeric": 0,
            "alpha_count": 0,
            "digit_count": 0,
            "ratio_alpha_to_numeric": float("inf"),  # same logic if digit_count=0
            "non_alphanumeric_count": 0,
            "non_ascii_count": 0,
            "ratio_non_ascii": 0,
            "capital_count": 0,
            "ratio_capital": 0,
            "digit_ratio": 0,
            "punctuation_count": 0,
            "ratio_punctuation": 0,
            "num_words": 0,
            "average_word_length": 0,
            "type_token_ratio": 0,  # text-level TTR
            "token_entropy": 0,  # text-level "word entropy"
            "char_entropy": 0,
            "num_copyright": 0,
            "num_rights_reserved": 0,
        }
    else:
        # ----------------------------
        # 2) Single pass over text (character-level)
        #    Collect a variety of counters.
        # ----------------------------
        whitespace_count = 0
        alpha_count = 0
        digit_count = 0
        capital_count = 0
        punctuation_count = 0
        non_ascii_count = 0

        # We'll also track lines and paragraphs in the same pass
        # (replicating .count("\n") and .count(".\r\n"), .count(".\n\n"))
        line_count = 1  # if there's any text, we start with 1
        paragraph_count = 0

        # Character frequency (for char_entropy)
        char_counts = {}

        i = 0
        while i < total_chars:
            c = text[i]

            # Update char_counts
            char_counts[c] = char_counts.get(c, 0) + 1

            # Whitespace?
            if c.isspace():
                whitespace_count += 1
                if c == "\n":
                    line_count += 1

            # Alphabetic?
            if c.isalpha():
                alpha_count += 1
                if c.isupper():
                    capital_count += 1
            # Digit?
            elif c.isdigit():
                digit_count += 1

            # Punctuation?
            if c in string.punctuation:
                punctuation_count += 1

            # Non-ASCII?
            if ord(c) > 127:
                non_ascii_count += 1

            # Check for paragraph indicators:
            # We replicate:
            #   num_paragraphs = text.count(".\r\n") + text.count(".\n\n") + 1
            # but do it in one pass:
            if c == ".":
                if i + 2 < total_chars:
                    nxt2 = text[i + 1 : i + 3]
                    if nxt2 == "\r\n" or nxt2 == "\n\n":
                        paragraph_count += 1
            i += 1

        paragraph_count += 1

        alphanumeric_count = sum(char_counts[c] for c in char_counts if c.isalnum())
        non_alphanumeric_count = total_chars - alphanumeric_count

        ratio_whitespace = whitespace_count / total_chars
        ratio_alphanumeric = alphanumeric_count / total_chars
        ratio_alpha_to_numeric = (
            (alpha_count / digit_count) if digit_count else float("inf")
        )
        ratio_non_ascii = non_ascii_count / total_chars
        ratio_capital = capital_count / alpha_count if alpha_count else 0
        digit_ratio = digit_count / total_chars
        ratio_punctuation = punctuation_count / total_chars

        average_line_length = total_chars / line_count if line_count else 0
        average_paragraph_length = (
            total_chars / paragraph_count if paragraph_count else 0
        )

        char_entropy = 0.0
        for c in char_counts:
            p = char_counts[c] / total_chars
            char_entropy += -p * math.log2(p)

        words = text.split()
        num_words = len(words)

        if num_words == 0:
            avg_word_length = 0
            text_level_ttr = 0
            text_token_entropy = 0
        else:
            word_counts = {}
            total_word_len = 0
            for w in words:
                total_word_len += len(w)
                word_counts[w] = word_counts.get(w, 0) + 1

            avg_word_length = total_word_len / num_words
            text_level_ttr = len(word_counts) / num_words

            # text-level "word entropy"
            text_token_entropy = 0.0
            for count in word_counts.values():
                p = count / num_words
                text_token_entropy += -p * math.log2(p)

        text_lower = text.lower()
        num_copyright = text_lower.count("copyright") + text_lower.count("©")
        num_rights_reserved = text_lower.count("rights reserved")

        text_metrics = {
            "total_characters": total_chars,
            "whitespace_count": whitespace_count,
            "ratio_whitespace": ratio_whitespace,
            "num_lines": line_count,
            "average_line_length": average_line_length,
            "num_paragraphs": paragraph_count,
            "average_paragraph_length": average_paragraph_length,
            "alphanumeric_count": alphanumeric_count,
            "ratio_alphanumeric": ratio_alphanumeric,
            "alpha_count": alpha_count,
            "digit_count": digit_count,
            "ratio_alpha_to_numeric": ratio_alpha_to_numeric,
            "non_alphanumeric_count": non_alphanumeric_count,
            "non_ascii_count": non_ascii_count,
            "ratio_non_ascii": ratio_non_ascii,
            "capital_count": capital_count,
            "ratio_capital": ratio_capital,
            "digit_ratio": digit_ratio,
            "punctuation_count": punctuation_count,
            "ratio_punctuation": ratio_punctuation,
            "num_words": num_words,
            "average_word_length": avg_word_length,
            "type_token_ratio": text_level_ttr,
            "token_entropy": text_token_entropy,
            "char_entropy": char_entropy,
            "num_copyright": num_copyright,
            "num_rights_reserved": num_rights_reserved,
        }

    total_tokens = len(tokens)

    if total_tokens == 0:
        token_metrics = {
            "total_tokens": 0,
            "unique_tokens": 0,
            "type_token_ratio": 0,
            "token_entropy": 0,
            "max_token_frequency_ratio": 0,
            "repetition_rate": 0,
            "num_nospace_bigrams": 0,
            "ratio_nospace_bigrams": 0,
            "num_format_tokens": 0,
            "ratio_format_tokens": 0,
            "startswith_begin": 0,
        }
    else:
        token_counts = {}
        num_nospace_bigrams = 0
        num_format_tokens = 0

        # Based on original code
        bad_bigram_token_ids = (35464, 67042, 108832)
        bad_format_token_ids = (
            395,
            477,
            1819,
            2098,
            12125,
            19220,
            25937,
            67199,
            126985,
            126997,
            127022,
            127034,
        )

        for i, t in enumerate(tokens):
            token_counts[t] = token_counts.get(t, 0) + 1
            # Check for bigram tokens
            if t in bad_bigram_token_ids:
                num_nospace_bigrams += 1
            # Check for format tokens
            if t in bad_format_token_ids:
                num_format_tokens += 1

        unique_tokens = len(token_counts)

        # ttr
        token_ttr = unique_tokens / total_tokens

        # entropy
        token_entropy = 0.0
        for count in token_counts.values():
            p = count / total_tokens
            token_entropy += -p * math.log2(p)

        max_freq = max(token_counts.values())
        max_token_freq_ratio = max_freq / total_tokens
        repetition_rate = 1 - (unique_tokens / total_tokens)
        token_metrics = {
            "total_tokens": total_tokens,
            "unique_tokens": unique_tokens,
            "type_token_ratio": token_ttr,
            "token_entropy": token_entropy,
            "max_token_frequency_ratio": max_token_freq_ratio,
            "repetition_rate": repetition_rate,
            "num_nospace_bigrams": num_nospace_bigrams,
            "ratio_nospace_bigrams": num_nospace_bigrams / total_tokens,
            "num_format_tokens": num_format_tokens,
            "ratio_format_tokens": num_format_tokens / total_tokens,
            "startswith_begin": 1 if tokens and tokens[0] == 47842 else 0,
        }

    metric_record = {**text_metrics, **token_metrics}

    # score
    quality_score = score_text(metric_record)
    token_adjusted_quality_score = quality_score / max(1, metric_record["total_tokens"])

    # merge and return
    return {
        "identifier": record.get("identifier", None),
        "mime_type": record.get("mime_type", None),
        "score": quality_score,
        "adjusted_score": token_adjusted_quality_score,
        **metric_record,
    }


if __name__ == "__main__":
    # set up args
    parser = argparse.ArgumentParser(description="Get metrics from a HF dataset.")

    # dataset name
    parser.add_argument(
        "dataset_name",
        type=str,
        help="The name of the HF dataset to analyze.",
    )

    # optional output name
    parser.add_argument(
        "--output_name",
        type=str,
        help="The name of the output file.",
        default="metrics.csv",
    )

    # optional limit number of records to sample
    parser.add_argument(
        "--limit",
        type=int,
        help="The number of records to sample from the dataset.",
        default=None,
    )

    # parse
    args = parser.parse_args()

    # load dataset
    dataset_name = args.dataset_name
    if dataset_name.endswith("*"):
        # filter from hf_api
        dataset_list = []
        for dataset in hf_api.list_datasets(author="alea-institute"):
            if dataset.id.startswith(dataset_name[:-1]):
                dataset_list.append(dataset.id)
    else:
        dataset_list = [dataset_name]

    # load all
    datasets = []
    for dataset_id in dataset_list:
        current_dataset = load_dataset(dataset_id, split="train", streaming=True)
        if args.limit:
            current_dataset = current_dataset.take(args.limit)
        datasets.append(current_dataset)

    # merge
    if len(datasets) > 1:
        # concatenate
        combined_dataset = concatenate_datasets(datasets)
    elif len(datasets) == 1:
        # just use the first one
        combined_dataset = datasets[0]
    else:
        raise ValueError("No datasets found.")

    # get metrics
    columns = []
    with open(args.output_name, "wt", encoding="utf-8") as output_file:
        csv_writer = csv.writer(output_file)
        for row in tqdm.tqdm(combined_dataset):
            metrics = get_metrics(row)
            if not columns:
                columns = list(metrics.keys())
                csv_writer.writerow(columns)
            csv_writer.writerow([metrics[c] for c in columns])
