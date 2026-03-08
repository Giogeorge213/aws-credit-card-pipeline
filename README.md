# aws-credit-card-pipeline

I like credit cards so I created an automated AWS pipeline that processes credit card PDF statements using Textract OCR, calculates rewards points, and stores transactions in PostgreSQL with QuickSight dashboards.

## Architecture

```
aws-credit-card-pipeline/
├── lambdas/
│   ├── chase_sapphire_processor/    # Chase Sapphire Preferred statements
│   │   └── lambda_function.py
│   └── chase_hyatt_processor/       # Chase World of Hyatt statements
│       └── lambda_function.py
├── database/
│   └── schema.sql                   # PostgreSQL schema
└── README.md
```

## Lambda Functions

### Chase Sapphire Preferred Processor
Processes Chase Sapphire Preferred credit card statements.
- **Trigger**: S3 upload to `chase-sapphire/` prefix
- **Points**: 3x Dining, 2x Transportation, 1x Everything else
- **Output**: CSV to `Processed/` folder + PostgreSQL insert

### Chase World of Hyatt Processor  
Processes Chase World of Hyatt credit card statements.
- **Trigger**: S3 upload to `chase-hyatt/` prefix
- **Points**: 4x Hyatt stays, 2x Dining/Airfare/Transit/Fitness, 1x Everything else
- **Output**: CSV to `Processed/` folder + PostgreSQL insert

## Features

- **PDF Text Extraction**: AWS Textract async document text detection
- **Transaction Parsing**: Extracts date, merchant, amount from statements
- **Merchant Categorization**: Auto-categorizes 20+ merchant categories
- **Points Calculation**: Card-specific rewards point calculations
- **Foreign Currency Support**: Handles PHP and other currency conversions
- **Database Storage**: PostgreSQL via pg8000

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DB_HOST` | PostgreSQL database host |
| `DB_NAME` | Database name |
| `DB_USER` | Database username |
| `DB_PASSWORD` | Database password |
| `DB_PORT` | Database port (default: 5432) |

## Dependencies

- `boto3` - AWS SDK for Python
- `pg8000` - Pure Python PostgreSQL driver

## Filename Convention

Statement PDFs should follow the format: `YYYYMMDD*.pdf` where the date prefix represents the statement date.