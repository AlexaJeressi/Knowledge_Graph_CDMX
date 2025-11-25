# Knowledge Graph CDMX - Legal Document Analysis System

A comprehensive data pipeline for constructing a knowledge graph from Mexico City's legal framework. This system extracts entities, relationships, and references from legal documents to create a structured network of laws, regulations, articles, and government entities.

[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## 🎯 Overview

This project processes legal documents from Mexico City (CDMX) to:
- Extract and identify legal entities, articles, and regulations
- Map relationships between laws, regulations, and government bodies
- Create a queryable knowledge graph of the legal framework
- Provide network analysis and visualizations of legal document clusters


## ✨ Key Features

- **Multi-source Text Extraction**: Processes DOCX and PDF legal documents
- **Entity Recognition**: Identifies laws, regulations, articles, and government entities
- **Relationship Mapping**: Links articles to documents, articles to articles, and regulations to laws
- **Reference Resolution**: Handles complex legal citations including:
  - Individual articles (e.g., "artículo 50")
  - Article ranges (e.g., "artículos del 10 al 15")
  - Relative references (e.g., "artículo anterior", "siguiente")
  - Latin suffixes (bis, ter, quáter, etc.)
- **Evaluation Framework**: Comprehensive metrics for pipeline validation
- **Network Analysis**: Clustering and visualization of legal document relationships

## 📊 Project Structure

```
Knowledge_Graph_CDMX/
│
├── data/
│   ├── 01_input/              # Raw legal documents (DOCX, PDF) and metadata
│   ├── 02_catalogs/           # Reference catalogs with unique identifiers
│   ├── 03_extracted/          # Extracted entities and mentions
│   ├── 04_matched/            # Matched relationships between entities
│   └── 05_output/             # Final relationship tables (edges for graph)
│
├── scripts/
│   ├── 00_text_extraction/    # Document scraping and text extraction
│   ├── 01_preprocessing/      # Hash generation and data cleaning
│   ├── 02_extraction/         # Entity and mention extraction
│   ├── 03_matching/           # Entity matching and relationship creation
│   ├── 04_output_tables/      # Final table generation
│   ├── 05_analysis/           # Network analysis and visualizations
│   ├── functions/             # Reusable extraction and matching functions
│   └── regex/                 # Regular expression patterns for entity detection
│
└── README.md
```

## 🚀 Getting Started

### Prerequisites

- Python 3.8 or higher
- Jupyter Notebook
- pip package manager

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/Knowledge_Graph_CDMX.git
cd Knowledge_Graph_CDMX
```

2. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install required packages:
```bash
pip install pandas numpy jupyter openpyxl requests beautifulsoup4 python-docx pypdf2 matplotlib seaborn networkx scikit-learn wordcloud openai
```

### Quick Start

1. **Text Extraction**: Extract text from legal documents
```bash
jupyter notebook scripts/00_text_extraction/extract_text.ipynb
```

2. **Entity Extraction**: Run the extraction pipeline
```bash
jupyter notebook scripts/02_extraction/
```

3. **Relationship Matching**: Create entity relationships
```bash
jupyter notebook scripts/03_matching/
```

4. **Generate Output**: Create final relationship tables
```bash
jupyter notebook scripts/04_output_tables/
```

## 🔄 Data Pipeline

### Stage 0: Text Extraction
- Scrapes legal documents from official CDMX sources
- Converts DOCX and PDF files to structured text
- Cleans and normalizes document formatting

### Stage 1: Preprocessing
- Generates unique hash identifiers for:
  - Articles (`art_hash.csv`)
  - Laws (`leyes_hash.csv`)
  - Government entities (`entes-publicos_hash.csv`)
- Creates catalog references for entity matching

### Stage 2: Extraction
Extracts six types of entities and mentions:

| Extraction Type | Description | F1-Score |
|----------------|-------------|----------|
| **Materia Mentions** | Subject matter references in legal texts | 1.000 |
| **Regulation Mentions** | References to regulations | 1.000 |
| **Self References** | Internal document references | 1.000 |
| **Official Laws & Entities** | Government bodies and agencies | 0.997 |
| **Legal Documents** | Generic legal document mentions | 0.931 |
| **Article Mentions** | Article citations and cross-references | 0.386* |

*Article mentions detection is challenging due to complex legal citation formats

### Stage 3: Matching
- Matches extracted entities to catalog identifiers
- Resolves ambiguous references using context
- Creates validated relationships between entities

### Stage 4: Output Tables
Generates final relationship tables:

- **art_art.csv**: Article-to-article relationships
- **art_doc.csv**: Article-to-document relationships
- **art_gov.csv**: Article-to-government entity relationships
- **art_parte_doc.csv**: Article-to-document section relationships
- **relaciones_reglamentos_leyes.csv**: Regulation-to-law relationships

### Stage 5: Analysis
- Network analysis of legal document clusters
- Word cloud generation for entity visualization
- Community detection in the legal framework network


## 📁 Output Format

All output tables follow a consistent format with:
- **Source identifiers**: Hash IDs for source entities
- **Target identifiers**: Hash IDs for target entities
- **Context**: Text snippets showing the relationship
- **Metadata**: Document names, article numbers, confidence scores

Example from `art_doc.csv`:
```csv
source_art_hash,target_doc_hash,mention_text,context,source_doc_name,target_doc_name
abc123,def456,"artículo 50","...según lo establecido en el artículo 50 de...",Ley de X,Ley de Y
```

## 🛠️ Key Functions

### Extraction Functions
Located in `scripts/functions/`:

- `article_mention_functions.py`: Extract article citations
- `entity_extraction_functions.py`: Extract government entities
- `legal_docs_functions.py`: Extract legal document references
- `regulation_mention_functions.py`: Extract regulation mentions
- `materia_mention_functions.py`: Extract subject matter references
- `self_reference_functions.py`: Extract internal document references

### Matching Functions
- `openai_law_matcher.py`: LLM-assisted entity disambiguation
- `filter_law_mentions.py`: Filter and validate law mentions
- `context_extraction.py`: Extract context windows for relationships

### Utility Functions
- `hash_functions.py`: Generate and manage unique identifiers

## 📊 Data Sources

Legal documents are sourced from:
- [Consejería Jurídica CDMX](https://data.consejeria.cdmx.gob.mx/)
- Official CDMX government publications
- City constitution and legislative documents


## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Note**: This project is for research and educational purposes. Always refer to official legal sources for authoritative legal information.

