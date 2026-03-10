# aws-credit-card-pipeline

I like credit cards so I created an automated AWS pipeline that processes credit card PDF statements using Textract OCR, calculates rewards points, and stores transactions in PostgreSQL with QuickSight dashboards.

## Architecture

```
PDF Upload → S3 → Lambda → Textract OCR → Parse Transactions
                                                ↓
                                    CSV (S3) + PostgreSQL
                                        ↓           ↓
                                  Glue Catalog  QuickSight
                                        ↓
                                     Athena
```

### AWS Services Used
- **S3** - Storage + event triggers
- **Lambda** - Serverless compute (Python 3.12)
- **Textract** - OCR text extraction
- **EC2** - PostgreSQL database
- **VPC** - Private subnets, endpoints, peering
- **IAM** - Roles and security policies
- **CloudWatch** - Logging and monitoring
- **QuickSight** - Dashboards and visualization
- **Glue** - Data catalog (crawler)
- **Athena** - Serverless SQL queries on S3

### Networking
- Custom VPC with private subnets (no IGW)
- VPC endpoints for S3 (free) and Textract
- VPC peering to connect Lambda to EC2 database
- Chose VPC endpoints over NAT Gateway for cost optimization ($7/month vs $32/month)

## Project Structure

```
aws-credit-card-pipeline/
├── lambdas/
│   ├── chase_sapphire_processor/
│   │   └── lambda_function.py
│   └── chase_hyatt_processor/
│       └── lambda_function.py
├── database/
│   └── schema.sql
├── queries/
│   └── sample_queries.sql
├── .env.example
└── README.md
```

## Lambda Functions

### Chase Sapphire Preferred Processor
- **Trigger:** S3 upload to `chase-ur/` prefix
- **Points:** 3x Dining, 2x Transportation, 1x Everything else
- **Output:** CSV to `Processed/` folder + PostgreSQL insert

### Chase World of Hyatt Processor
- **Trigger:** S3 upload to `chase-hyatt/` prefix
- **Points:** 4x Hyatt stays, 2x Dining/Airfare/Transit/Fitness, 1x Everything else
- **Output:** CSV to `Processed/` folder + PostgreSQL insert

## Features
- **PDF Text Extraction:** AWS Textract async document text detection with polling
- **Transaction Parsing:** Regex-based extraction of date, merchant, amount
- **Merchant Categorization:** 20+ categories (Dining, Travel, Hotels, etc.)
- **Points Calculation:** Card-specific rewards point logic
- **Foreign Currency:** PHP exchange rate detection
- **Duplicate Prevention:** Delete-then-insert by card type + statement date
- **Data Catalog:** Glue crawler catalogs CSVs for Athena queries
- **Dashboards:** QuickSight connected to PostgreSQL (direct query)

## Technical Challenges

### VPC Networking
Lambda in public subnets couldn't reach Textract (no public IP, can't use IGW). Created custom VPC with private subnets and VPC endpoints. Used VPC peering to connect Lambda to the database in the original VPC.

### Lambda Layer
psycopg2's compiled C extensions require Lambda's exact OS and Python version. After multiple failed builds on Windows, Mac, and CloudShell, chose pg8000 (pure Python PostgreSQL driver) which works in Lambda layers without C dependencies.

## Environment Variables

| Variable | Description |
|----------|-------------|
| DB_HOST | PostgreSQL database host |
| DB_NAME | Database name |
| DB_USER | Database username |
| DB_PASSWORD | Database password |
| DB_PORT | Database port (default: 5432) |

## Dependencies
- **boto3** - AWS SDK for Python
- **pg8000** - Pure Python PostgreSQL driver (Lambda layer)

## Filename Convention
Statement PDFs should follow the format: `YYYYMMDD*.pdf` where the date prefix represents the statement date.

## Cost

| Service | Monthly Cost |
|---------|-------------|
| EC2 (t2.micro) | ~$9 |
| Textract VPC Endpoint | ~$7 |
| S3, Lambda, Glue, Athena | < $1 |
| **Total** | **~$17/month** |