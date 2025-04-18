# KL3M Training Data
## Collection and Preprocessing of Training Data for KL3M

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)


## Description

This [ALEA](https://aleainstitute.ai/) project contains the complete source code to collect and preprocess
all training data related to the [KL3M embedding and generative models](https://kl3m.ai/). The KL3M Data Project 
provides a comprehensive, copyright-clean dataset for training large language models, addressing legal risks in 
AI data collection.

### Key Features
- Over 132 million documents spanning trillions of tokens
- Verifiably public domain or appropriately licensed sources
- Complete source code for document acquisition and processing
- Multi-stage data access with original formats, extracted content, and pre-tokenized representations


## Paper
[The KL3M Data Project: Copyright-Clean Training Resources for Large Language Models](https://arxiv.org/html/2504.07854v1)

## Dataset
[Hugging Face Dataset: kl3m-data-snapshot-20250324](https://huggingface.co/datasets/alea-institute/kl3m-data-snapshot-20250324)

## Citation
```bibtex
@misc{bommarito2025kl3mdata,
  title={The KL3M Data Project: Copyright-Clean Training Resources for Large Language Models},
  author={Bommarito II, Michael J. and Bommarito, Jillian and Katz, Daniel Martin},
  year={2025},
  eprint={2504.07854},
  archivePrefix={arXiv},
  primaryClass={cs.CL}
}
```

## Primary Sources

### Summary
TODO: Table


### US

* [x] us/dockets: PACER/RECAP docket sheets via archive.org
* [x] us/dotgov: filtered .gov TLD domains via direct retrieval
* [x] us/ecfr: Electronic Code of Federal Regulations (eCFR) via NARA/GPO API
* [x] us/edgar: SEC EDGAR data via SEC feed
* [x] us/fdlp: US Federal Depository Library Program (FDLP) via GPO
* [x] us/fr: Federal Register data via NARA/GPO API
* [x] us/govinfo: US Government Publishing Office (GPO) data via GovInfo API
* [x] us/recap: RECAP raw documents via S3
* [x] us/recap_docs: RECAP attached docs (Word, WordPerfect, PDF, MP3) via S3
* [x] us/reg_docs: Documents associated with regulations.gov dockets via regulations.gov API
* [x] us/usc: US Code releases via Office of the Law Revision Counsel (OLRC)
* [x] us/uspto_patents: USPTO patent grants via USPTO bulk data


### EU ("Federal")

 * [x] eu/eurlex_oj: EU Official Journal via Cellar/Europa

### UK

 * [x] uk/legislation: All enacted UK legislation via legislation.gov.uk bulk download


### Germany

 * [ ] de/bundesgesetzblatt: Bundesgesetzblatt (BGBl) 2023- from recht.bund.de


### Australia

### Canada

### India

## Tasks

### Extraction


### Summarization


### Transform and Convert



## Installation

```bash
# Clone the repository
git clone https://github.com/alea-institute/kl3m-data.git
cd kl3m-data

# Install dependencies using Poetry
poetry install
```

## Usage

### Accessing the Dataset
The KL3M dataset is available through multiple channels:

1. **Hugging Face**:
   ```python
   from datasets import load_dataset
   dataset = load_dataset("alea-institute/kl3m-data-snapshot-20250324")
   ```

2. **S3 Bucket**:
   ```bash
   aws s3 ls s3://data.kl3m.ai/
   ```

3. **Project Website**:
   Visit [https://gallery.kl3m.ai/](https://gallery.kl3m.ai/) for more information.

## License

The source code for this ALEA project is released under the MIT License. See the [LICENSE](LICENSE) file for details.

Top-level dependencies are all licensed MIT, BSD-3, or Apache 2.0  See `poetry show --tree` for details.

## Support

If you encounter any issues or have questions about using this ALEA project, please [open an issue](https://github.com/alea-institute/kl3m-data/issues) on GitHub.

## Learn More

To learn more about ALEA and our KL3M models and data, visit the [ALEA website](https://aleainstitute.ai/).
