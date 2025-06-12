# IBM Knowledge Catalog (IKC) Bulk Guru

This repo demonstrates how the IKC API (formerly Watson Data Platform API) can be used to perform quick updates in bulk for some laborious tasks when done through the UI.

## Prerequisites and Recommended Tools

- Cloud Pak for Data `5.0` or above
- Github Desktop <https://desktop.github.com/>
- Git (optional for integration with VSC) <https://git-scm.com/downloads>
- Python `3.8` or above <https://www.python.org/downloads/>
- Visual Studio Code (VSC) <https://code.visualstudio.com/>
- Python Extension of VSC <https://code.visualstudio.com/docs/python/python-tutorial>

## Installation

1. Make sure the prerequisites are installed.
2. Clone this github repository.
3. Install necessary dependencies:

```sh
pip install -r requirements.txt
```

4. Create `.env` file with Secrets:

```sh
cp .envExample .env
```

5. Configure your `.env` file with your CPD environment details:

```env
CPD_URL=https://your-cpd-cluster.com
CPD_USERNAME=your-username
CPD_PASSWORD=your-password
CATALOG_ID=your-catalog-id
```

### Swagger UIs and other API docs

- <https://{cp4d_hostname}/v3/search/api/explorer>
- <https://cloud.ibm.com/apidocs/watson-data-api>

### Programming resources

- Beginners Guide to Python: <https://wiki.python.org/moin/BeginnersGuide/NonProgrammers>
- Programmers Guide to Python: <https://wiki.python.org/moin/BeginnersGuide/Programmers>
- Getting Started with Pandas: <https://pandas.pydata.org/docs/getting_started/index.html>

## Use Cases

Following are the key use cases that are covered in this repo:

### How do I authenticate?

- Session class
- Hostname, User, Pwd or API Key
- Headers, Token

### How do I generate an API Key?

https://www.ibm.com/docs/en/cloud-paks/cp-data/5.1.x?topic=tutorials-generating-api-keys#api-keys__platform

### How do I search for artifacts?

Using Global Search to demonstrate how to find and export artifacts in IKC.

#### Export Artifacts to CSV

Use `export_artifacts.py` to export all governance artifacts from your CPD environment to CSV files:

```sh
python export_artifacts.py
```

This will create timestamped CSV files in the `exports/` directory:
- `glossary_term_YYYYMMDD_HHMMSS.csv`
- `data_class_YYYYMMDD_HHMMSS.csv`
- `classification_YYYYMMDD_HHMMSS.csv`

**CSV Output Columns:**
- `artifact_id` - Unique identifier for the artifact
- `categories.primary_category_name` - Primary category assignment
- `entity.artifacts.global_id` - Global identifier
- `metadata.name` - Artifact name

### How do I perform mass (bulk) updates?

#### Bulk Assignment of Terms, Classifications, and Data Classes

Use the main bulk assignment script to assign governance artifacts to asset columns in bulk.

#### Prepare a CSV

Create a CSV file with the following format (no header row required):

```csv
Asset Name,Column Name,Term Name,Term Category,Classification Name,Classification Category,Data Class Name,Data Class Category
```

**Example CSV content:**
```csv
customer_data,customer_id,Customer Identifier,Customer Data,Personally Identifiable Information,Data Privacy,Person Identifier,Data Classification
sales_data,email_address,Email Address,Contact Information,Personal Data,Data Privacy,Email,Data Classification
product_catalog,product_name,Product Name,Product Data,,,,
```

**Column Descriptions:**
- **Asset Name**: Exact name of the data asset in your catalog or project
- **Column Name**: Exact name of the column within the asset
- **Term Name**: Business term to assign (leave empty to skip)
- **Term Category**: Category where the term is located
- **Classification Name**: Classification to assign (leave empty to skip)
- **Classification Category**: Category where the classification is located
- **Data Class Name**: Data class to assign (leave empty to skip)
- **Data Class Category**: Category where the data class is located

#### Run the Bulk Assignment

```sh
python bulk_assign_project.py
```

By default, it looks for `col_term_map.csv` in the current directory. You can modify the script to use a different input file.

**The script will:**
1. **Preload all artifacts** into memory for fast lookups
2. **Process each row** in your CSV file
3. **Validate** that assets and columns exist
4. **Assign** terms, classifications, and data classes as specified
5. **Generate** a detailed results CSV with success/failure status

#### Understanding the Output

The script creates an output file `{input_filename}_out.csv` with additional columns:
- **Term Result**: SUCCESS, ERROR, or SKIPPED
- **Classification Result**: SUCCESS, ERROR, or SKIPPED  
- **Data Class Result**: SUCCESS, ERROR, or SKIPPED
- **Asset Update Status**: Overall update status

**Example output:**
```csv
customer_data,customer_id,Customer Identifier,Customer Data,PII,Data Privacy,Person ID,Data Classes,SUCCESS,SUCCESS,SUCCESS,SUCCESS
sales_data,invalid_column,Email,Contacts,PII,Privacy,Email,Classes,ERROR: Column 'invalid_column' not found in asset,ERROR: Column 'invalid_column' not found in asset,ERROR: Column 'invalid_column' not found in asset,ERROR: Column 'invalid_column' not found in asset
```

## Scripts Overview

### `export_artifacts.py`
Exports all governance artifacts (terms, classifications, data classes) to CSV files for analysis or backup.

**Features:**
- Handles pagination for large datasets
- Exports to timestamped CSV files

### `bulks_assign_project.py` and `bulks_assign_catalog.py` (Bulk Assignment)
Performs bulk assignment of governance artifacts to asset columns.

**Features:**
- **Smart caching**: Preloads all artifacts to minimize API calls
- **Validation**: Checks asset and column existence before updates
- **Granular tracking**: Reports success/failure for each assignment type
- **Incremental updates**: Only updates specified fields, preserves existing metadata
- **Detailed logging**: Console output shows progress and issues
- **Results tracking**: Generates comprehensive output CSV

## Best Practices

### Data Preparation
- **Exact naming**: Asset and column names must match exactly (case-sensitive)
- **Validate categories**: Ensure term/classification/data class categories exist
- **Test with small batches**: Start with 10-20 rows to validate your CSV format

### Error Handling
- **Check output CSV**: Always review the results file for any failures
- **Common errors**:
  - Asset not found: Check asset name spelling and catalog access
  - Column not found: Verify column exists in asset schema
  - Artifact not found: Confirm term/classification/data class names and categories

## Troubleshooting

### Authentication Issues
```
Error: 401 Unauthorized
```
- Verify CPD credentials in `.env` file
- Check if API key is valid and not expired
- Ensure user has necessary permissions

### Asset Not Found
```
Asset 'asset_name' is either not found or duplicated
```
- Verify asset name matches exactly (case-sensitive)
- Check catalog_id is correct
- Ensure asset is in PUBLISHED state

### Column Not Found
```
Column 'column_name' not found in asset
```
- Verify column name matches schema exactly
- Check if asset has been profiled/discovered
- Ensure column_info exists in asset metadata

### Artifact Lookup Failures
```
Term 'term_name' with category 'category' not found
```
- Verify artifact names and categories are exact matches
- Check if artifacts are in PUBLISHED state
- Run `export_artifacts.py` to see available artifacts
