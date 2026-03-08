# aws-credit-card-pipeline

I like credit cards so I created an automated AWS pipeline that processes credit card PDF statements using Textract OCR, calculates rewards points, and stores transactions in PostgreSQL with QuickSight dashboards

## Chase Statement Processor Lambda Function

AWS Lambda function that processes Chase credit card PDF statements using Amazon Textract, extracts transaction data, categorizes merchants, calculates rewards points, and stores results in a PostgreSQL database.

### Features

- **PDF Text Extraction**: Uses AWS Textract for async document text detection
- **Transaction Parsing**: Extracts transactions from Chase Sapphire Preferred statements
- **Merchant Categorization**: Auto-categorizes merchants (Dining, Airlines, Hotels, etc.)
- **Points Calculation**: Calculates Chase Ultimate Rewards points based on category multipliers
- **Foreign Currency Support**: Handles Philippine Peso conversions
- **Database Storage**: Stores transactions in PostgreSQL using pg8000
- **CSV Export**: Generates CSV files with extracted data

### Environment Variables

| Variable | Description |
|----------|-------------|
| `DB_HOST` | PostgreSQL database host |
| `DB_NAME` | Database name |
| `DB_USER` | Database username |
| `DB_PASSWORD` | Database password |
| `DB_PORT` | Database port (default: 5432) |

### Dependencies

- `boto3` - AWS SDK for Python
- `pg8000` - Pure Python PostgreSQL driver

### Trigger

S3 trigger on PDF uploads. The filename should follow the format: `YYYYMMDD*.pdf` where the date prefix represents the statement date.

### Output

- Processed CSV files uploaded to `Processed/` folder in S3
- Debug extracted text saved to `debug/` folder
- Transactions inserted into PostgreSQL `transactions` table

### Points Calculation

- **Dining/Fast Food**: 3x points
- **Transportation**: 2x points  
- **All other categories**: 1x points