# KL3M Document Parsing System

This document provides an overview of the document parsing system used in the KL3M dataset. The parsing system is responsible for converting raw document content from various formats into standardized representations that can be tokenized and used for training language models.

## Architecture Overview

The KL3M parsing system is designed to handle a wide variety of document formats from different sources while producing consistent, normalized output. The system follows a modular architecture with the following components:

1. **Central Parsing Logic** - Orchestrates the parsing process
2. **Format-Specific Parsers** - Handle different file formats 
3. **Content Filters** - Clean and normalize document content
4. **Tokenizers** - Convert text to token sequences
5. **Document Representation** - Standardized format for parsed documents

## Core Components

### Parser Types

The `ParsedDocument` and `ParsedDocumentRepresentation` classes (in `parser_types.py`) define the core data structures used throughout the parsing system:

- **ParsedDocument** - Contains document metadata and multiple representations
  - `source` - Origin of the document
  - `identifier` - Unique document identifier
  - `original_uri` - Original S3 URI of the document
  - `representations` - Dictionary of content representations
  - `metadata` - Additional document metadata
  - `success` - Parsing status flag
  - `error` - Error message if parsing failed

- **ParsedDocumentRepresentation** - Contains a specific representation of document content
  - `content` - The actual text content
  - `tokens` - Dictionary mapping tokenizer name to token lists
  - `mime_type` - Format of the content (e.g., "text/plain", "text/markdown")

### Central Parsing Logic

The central parsing logic in `parser.py` handles:

1. **Object Retrieval** - Getting document content from S3
2. **Content Preparation** - Decompressing and normalizing content
3. **Format Detection** - Determining document format and selecting appropriate parser
4. **Document Parsing** - Delegating to format-specific parsers
5. **Post-processing** - Applying filters and tokenizing content
6. **Representation Storage** - Managing the resulting document representations

The main function, `parse_object()`, coordinates the entire process:

```python
def parse_object(client, bucket, key, max_size=None) -> List[ParsedDocument]:
    # Get object data from S3
    object_data = get_object_data(client, bucket, key)
    
    # Parse the content based on format
    documents = parse_content(
        object_content=object_data["content"],
        object_source=object_data.get("source"),
        object_format=object_data.get("format"),
        object_url=object_uri,
    )
    
    # Apply post-processing and filters
    for document in documents:
        document = postprocess_document(document, object_uri)
        # Add to final documents if valid
        
    return final_documents
```

### Format-Specific Parsers

The KL3M system includes parsers for common document formats:

#### Generic PDF Parser (`generic_pdf.py`)

Handles PDF documents using a multi-stage approach:
1. Detects PDF type (text, mixed, image)
2. For digital PDFs, extracts text and markdown using the `alea_preprocess` library
3. Falls back to Apache Tika for complex PDFs
4. Uses Poppler and Tesseract for OCR if needed for image-only PDFs

Example flow:
```python
# Detect PDF type
pdf_type = alea_preprocess.parsers.pdf.detection.detect_buffer_type(content)

# Choose appropriate parsing method
if pdf_type in (Text, Mixed, ImagePostOCR):
    documents = parse_digital_pdf(content, source, identifier)
else:
    documents = parse_tika(content, source, identifier)

# Try OCR if other methods fail
if not documents:
    documents = parse_poppler_tesseract(content, source, identifier)
```

#### Generic HTML Parser (`generic_html.py`)

Extracts meaningful content from HTML documents:
- Removes boilerplate content, navigation, ads, etc.
- Preserves document structure in markdown
- Extracts metadata from HTML tags

#### Generic XML Parser (`generic_xml.py`)

Processes structured XML documents:
- Handles different XML namespaces
- Supports XSLT transformations
- Extracts content and metadata

#### Generic JSON Parser (`generic_json.py`)

Extracts meaningful content from JSON data:
- Handles different JSON structures
- Extracts text fields and metadata

#### Generic ZIP Parser (`generic_zip.py`)

Extracts and processes content from ZIP archives:
- Handles nested archives
- Processes each file with appropriate parser

#### Specialized Parsers

For specific data sources, the system includes specialized parsers:

- **EU Official Journal XML Parser** (`eu_oj_xml.py`) - Specialized for EU legislation format
- **Tika Integration** (`generic_tika.py`) - Apache Tika for complex formats

### Content Filters

Filters apply normalization and cleaning rules to document content:

#### Verdate Filter (`filters/verdate.py`)

Removes standardized date lines from government documents:
```python
def filter_buffer(buffer):
    """Filter out lines starting with 'VerDate'"""
    return "\n".join(
        line for line in buffer.split("\n") if not line.startswith("VerDate")
    )
```

Filters are applied during post-processing:
```python
# Apply filters to content
for filter_func in DEFAULT_FILTERS:
    representation_data.content = filter_func.filter_buffer(
        representation_data.content
    )
```

### Tokenization

After parsing and filtering, documents are tokenized using the specified tokenizers:

```python
# Tokenize each representation
for tokenizer in DEFAULT_TOKENIZERS:
    representation_data.tokens[tokenizer] = (
        alea_preprocess.algos.tokenizers.encode_str(
            tokenizer,
            representation_data.content,
        )
    )
```

The default tokenizer is `alea-institute/kl3m-004-128k-cased`.

## Parsing Process Flow

1. **Object Retrieval**
   - Document content is retrieved from S3
   - Content is decompressed from base64-encoded zlib format

2. **Format Detection**
   - Content type is determined from metadata or by examining content
   - Appropriate parser is selected based on format

3. **Document Parsing**
   - Format-specific parser extracts text content
   - Multiple representations may be created (plain text, markdown, etc.)
   - Metadata is extracted

4. **Content Filtering**
   - Filters are applied to clean and normalize content
   - Empty content is filtered out

5. **Tokenization**
   - Text is tokenized with specified tokenizers
   - Token counts are stored with representations

6. **Result Storage**
   - Processed document representations are stored in the `representations` S3 stage
   - Content is compressed for storage

## Common Parser Strategies

### Text Extraction Strategy

Different approaches are used based on document type:

- **Plain Text** - Used directly with minimal processing
- **Markdown** - Normalized using `MarkdownNormalizer`
- **HTML** - Converted to clean markdown with structure preserved
- **PDF** - Uses digital text extraction, Tika, or OCR depending on PDF type
- **XML** - Transformed to structured text preserving important elements
- **JSON** - Text fields extracted and normalized
- **ZIP** - Contents extracted and processed recursively

### OCR Process

For image-based PDFs or when text extraction fails:

1. PDF is converted to PNG images using `pdftocairo`
2. Images are processed with Tesseract OCR
3. OCR text is normalized and combined into a single document

### Fallback Strategy

The system implements multiple fallback strategies:

1. Primary parsers attempt to extract text directly
2. If unsuccessful, Apache Tika is used as a fallback
3. If Tika fails, OCR is attempted for supported formats
4. If all extraction methods fail, the document is skipped

## Adding New Parsers

To add support for a new document format:

1. Create a new parser module (e.g., `generic_newformat.py`)
2. Implement the `parse()` function that returns a list of `ParsedDocument` objects
3. Update the format detection logic in `generic_object.py` to recognize the new format
4. Add any necessary format-specific filters

Example parser structure:
```python
def parse(content, source=None, identifier=None) -> List[ParsedDocument]:
    # Extract content from the format
    extracted_text = extract_from_format(content)
    
    # Create representations
    return [
        ParsedDocument(
            source=source,
            identifier=identifier,
            success=True,
            representations={
                "text/plain": ParsedDocumentRepresentation(
                    content=extracted_text,
                    mime_type="text/plain",
                ),
            },
        )
    ]
```

## Common Issues and Solutions

### Empty Content

Documents with empty content after filtering are skipped:
```python
if len(representation_data.content.strip()) == 0:
    continue
```

### Large Documents

Documents exceeding the maximum size limit are skipped:
```python
if max_size is not None and len(object_data["content"]) > max_size:
    LOGGER.error("Content is larger than current max size setting")
    return []
```

### Format Detection Errors

If format detection fails, the system attempts to determine the format from content:
```python
if object_format == "application/octet-stream":
    content_info = alea_preprocess.io.fs.file_info.get_file_info_from_buffer(
        object_data["content"]
    )
    object_format = content_info.media_type
```

### Parsing Failures

If a parser fails, an error is logged and the document is skipped:
```python
try:
    document = postprocess_document(document, object_uri)
    final_documents.append(document)
except Exception as e:
    LOGGER.error("Error postprocessing document %s: %s", object_uri, e)
```

## Performance Considerations

- Large documents may require significant processing time, especially for OCR
- Complex formats like PDF with mixed content take longer to process
- Tokenization time scales with document length
- Some parsers (Tika, OCR) may require external processes

## Further Reading

- [DATA.md](./DATA.md) - Overview of the data pipeline
- [SOURCES.md](./SOURCES.md) - Information about data sources