import json
import boto3
import csv
import io
import re
import time
import os
import pg8000.native as pg8000
from datetime import datetime
from urllib.parse import unquote_plus

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = int(os.environ.get('DB_PORT', '5432'))

s3_client = boto3.client('s3')
textract = boto3.client('textract')

def insert_transactions(transactions, card_type, stmt_date):
   conn = pg8000.Connection(
       host=DB_HOST, database=DB_NAME,
       user=DB_USER, password=DB_PASSWORD, port=DB_PORT
   )
   
   conn.run("DELETE FROM transactions WHERE card_type = :ct AND statement_date = :sd",
            ct=card_type, sd=stmt_date)
   
   for txn in transactions:
       date_str = txn['date']
       if len(date_str) == 5:  # MM/DD format
           date_str = f"2025-{date_str[:2]}-{date_str[3:]}"
       elif len(date_str) == 8:  # MM/DD/YY format
           parts = date_str.split('/')
           date_str = f"20{parts[2]}-{parts[0]}-{parts[1]}"
       
       conn.run("""
           INSERT INTO transactions 
           (transaction_date, merchant, amount, transaction_type, category, 
            card_type, points_earned, points_program, foreign_amount, 
            exchange_rate, currency, statement_date)
           VALUES (:date, :merchant, :amount, :type, :category, 
                   :card_type, :points, :program, :foreign_amt, :rate, :currency, :stmt_date)
       """, date=date_str, merchant=txn['merchant'], amount=float(txn['amount']),
            type=txn['type'], category=txn['category'], card_type=card_type,
            points=txn['points_earned'], program=txn['points_program'],
            foreign_amt=txn.get('foreign_amount'), rate=txn.get('exchange_rate'),
            currency=txn.get('currency', 'USD'), stmt_date=stmt_date)
   
   conn.close()

def lambda_handler(event, context):
   try:
       bucket = event['Records'][0]['s3']['bucket']['name']
       key = unquote_plus(event['Records'][0]['s3']['object']['key'])
       
       print(f"Processing file from bucket: {bucket}")
       
       # Extract statement date from filename
       filename = key.split('/')[-1]
       stmt_date = f"{filename[:4]}-{filename[4:6]}-{filename[6:8]}"
       print(f"Statement date: {stmt_date}")
       
       # Start async Textract job
       response = textract.start_document_text_detection(
           DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
       )
       
       job_id = response['JobId']
       print(f"Started Textract job: {job_id}")
       
       # Poll for completion
       max_attempts = 60
       for attempt in range(max_attempts):
           result = textract.get_document_text_detection(JobId=job_id)
           status = result['JobStatus']
           
           if status == 'SUCCEEDED':
               full_text = "\n".join([
                   block['Text'] 
                   for block in result['Blocks'] 
                   if block['BlockType'] == 'LINE'
               ])
               
               next_token = result.get('NextToken')
               while next_token:
                   result = textract.get_document_text_detection(
                       JobId=job_id, NextToken=next_token
                   )
                   full_text += "\n" + "\n".join([
                       block['Text'] 
                       for block in result['Blocks'] 
                       if block['BlockType'] == 'LINE'
                   ])
                   next_token = result.get('NextToken')
               
               break
           elif status == 'FAILED':
               raise Exception(f"Textract job failed: {result.get('StatusMessage', 'Unknown error')}")
           
           time.sleep(2)
       else:
           raise Exception("Textract job timed out after 120 seconds")
       
       # Save debug text
       s3_client.put_object(
           Bucket=bucket,
           Key=f"debug/{filename}_extracted_text.txt",
           Body=full_text
       )
       
       # Extract data
       extracted_data = extract_chase_statement_data(full_text)
       
       # Create CSV
       csv_content = create_csv_content(extracted_data)
       
       # Insert into database
       insert_transactions(extracted_data['transactions'], 'Chase Sapphire Preferred', stmt_date)
       
       # Upload CSV
       output_filename = filename.replace('.pdf', '_processed.csv')
       output_key = f"Processed/{output_filename}"
       
       s3_client.put_object(
           Bucket=bucket, Key=output_key,
           Body=csv_content, ContentType='text/csv'
       )
       
       print(f"Successfully processed and uploaded to Processed/ folder")
       
       return {
           'statusCode': 200,
           'body': json.dumps({
               'message': 'PDF processed successfully',
               'output_file': f's3://{bucket}/{output_key}',
               'transactions_extracted': len(extracted_data.get('transactions', []))
           })
       }
   
   except Exception as e:
       print(f"Error processing PDF: {str(e)}")
       raise

def extract_chase_statement_data(full_text):
   data = {
       'account_info': extract_account_info(full_text),
       'transactions': extract_transactions(full_text),
       'summary': extract_summary_info(full_text)
   }
   
   if not data['transactions']:
       print("WARNING: No transactions extracted")
   return data

def extract_account_info(text):
   account_info = {}
   
   account_match = re.search(r'Account Number:\s*(XXXX XXXX XXXX \d{4})', text)
   if account_match:
       account_info['account_number'] = account_match.group(1)
   
   period_match = re.search(r'Opening/Closing Date(\d{2}/\d{2}/\d{2})\s*-\s*(\d{2}/\d{2}/\d{2})', text)
   if period_match:
       account_info['statement_start'] = period_match.group(1)
       account_info['statement_end'] = period_match.group(2)
   
   new_balance_match = re.search(r'New Balance\$?([\d,]+\.[\d]{2})', text)
   if new_balance_match:
       account_info['new_balance'] = float(new_balance_match.group(1).replace(',', ''))
   
   previous_balance_match = re.search(r'Previous Balance\$?([\d,]+\.[\d]{2})', text)
   if previous_balance_match:
       account_info['previous_balance'] = float(previous_balance_match.group(1).replace(',', ''))
   
   credit_limit_match = re.search(r'Credit Access Line\$?([\d,]+)', text)
   if credit_limit_match:
       account_info['credit_limit'] = float(credit_limit_match.group(1).replace(',', ''))
   
   due_date_match = re.search(r'Payment Due Date:(\d{2}/\d{2}/\d{2})', text)
   if due_date_match:
       account_info['payment_due_date'] = due_date_match.group(1)
   
   min_payment_match = re.search(r'Minimum Payment Due:\$?([\d,]+\.[\d]{2})', text)
   if min_payment_match:
       account_info['minimum_payment'] = float(min_payment_match.group(1).replace(',', ''))
   
   return account_info

def extract_transactions(text):
   transactions = []
   card_type = 'Chase Sapphire Preferred'
   
   activity_start = text.find('ACCOUNT ACTIVITY')
   if activity_start == -1:
       return transactions
   
   activity_text = text[activity_start:]
   
   activity_end = activity_text.find('2025 Totals')
   if activity_end != -1:
       activity_text = activity_text[:activity_end]
   
   lines = activity_text.split('\n')
   
   i = 0
   while i < len(lines):
       line = lines[i].strip()
       
       if re.match(r'^\d{2}/\d{2}$', line):
           date_str = line
           
           if i + 1 < len(lines):
               merchant = lines[i + 1].strip()
               
               if merchant.upper() in ['PAYMENTS AND OTHER CREDITS', 'PURCHASE', 'CASH ADVANCES']:
                   i += 1
                   continue
               
               if i + 2 < len(lines):
                   amount_line = lines[i + 2].strip()
                   amount_match = re.match(r'^(-?[\d,]+\.\d{2})$', amount_line)
                   
                   if amount_match:
                       amount_str = amount_match.group(1)
                       
                       try:
                           amount = float(amount_str.replace(',', ''))
                       except ValueError:
                           i += 1
                           continue
                       
                       foreign_amount = None
                       exchange_rate = None
                       currency = 'USD'
                       
                       if i + 3 < len(lines):
                           next_line = lines[i + 3].strip()
                           if 'PHILIPPINE PESO' in next_line or 'PESO' in next_line:
                               if i + 4 < len(lines):
                                   exchange_line = lines[i + 4].strip()
                                   peso_match = re.search(r'([\d,]+\.\d{2})\s+X\s+([\d.]+)', exchange_line)
                                   if peso_match:
                                       foreign_amount = float(peso_match.group(1).replace(',', ''))
                                       exchange_rate = float(peso_match.group(2))
                                       currency = 'PHP'
                                       i += 2
                       
                       transaction_type = 'Credit' if amount < 0 else 'Purchase'
                       category = categorize_merchant(merchant)
                       points_earned, points_program = calculate_points(amount, category)
                       
                       transactions.append({
                           'date': date_str,
                           'merchant': merchant,
                           'amount': amount,
                           'type': transaction_type,
                           'category': category,
                           'card_type': card_type,
                           'points_earned': points_earned,
                           'points_program': points_program,
                           'foreign_amount': foreign_amount,
                           'exchange_rate': exchange_rate,
                           'currency': currency
                       })
                       
                       i += 3
                       continue
       
       i += 1
   
   statement_year = 2025
   year_match = re.search(r'Statement Date:\s*(\d{2}/\d{2}/(\d{2}))', text)
   if year_match:
       statement_year = 2000 + int(year_match.group(2))
   
   for transaction in transactions:
       try:
           month_day = transaction['date']
           full_date = datetime.strptime(f"{statement_year}/{month_day}", "%Y/%m/%d")
           transaction['date'] = full_date.strftime("%m/%d/%y")
       except:
           pass
   
   return transactions

def categorize_merchant(merchant):
   merchant_upper = merchant.upper()
   
   # Payments/Credits
   if any(x in merchant_upper for x in ['PAYMENT', 'THANK YOU', 'ANNUAL MEMBERSHIP', 'ANNUAL HOTEL CREDIT', 'TRANSACTION FEE']):
       return 'Payment/Fee'
   
   # Amazon
   if any(x in merchant_upper for x in ['AMAZON', 'AMZN', 'AMZ*', 'AWS']):
       return 'Amazon/Online Shopping'
   
   # Airlines/Flights
   if any(x in merchant_upper for x in ['JETBLUE', 'DELTA', 'UNITED', 'AIR CAN', 'CEBU AIR', 'SOUTHWEST', 'AMERICAN AIR']):
       return 'Airlines'
   
   # Hotels/Lodging
   if any(x in merchant_upper for x in ['HYATT', 'MARRIOTT', 'COURTYARD', 'SHERATON', 'HILTON', 'HOTEL', 'UNDER CANVAS', 'VENETIAN', 'PALAZZO', 'TIKI TIKI RESORTS', 'IM HOTEL', 'HOTWIRE', 'BOOKING.COM', 'TRIP.COM']):
       return 'Hotels/Lodging'
   
   # Fast Food
   if any(x in merchant_upper for x in ['IN-N-OUT', 'JACK IN THE BOX', 'MCDONALD', 'MC DONALD', 'WHATABURGER', 'WENDY', 'BURGER KING', 'PANDA EXPRESS', 'DEL TACO', 'RAISING CANE', 'WAFFLE HOUSE', 'CRACKER BARREL', 'JERSEY MIKE', 'FREEBIRDS', 'CHILI', 'BUFFALO WW']):
       return 'Fast Food'
   
   # Dining/Restaurants/Bars
   if any(x in merchant_upper for x in ['RESTAURANT', 'CAFE', 'COFFEE', 'PIZZA', 'GRILL', 'BISTRO', 'DINER', 'KITCHEN', 'BAR', 'PUB', 'BREWERY', 'TAVERN', 'STARBUCKS', 'CHIPOTLE', 'SUSHI', 'TST*', 'SQ *', 'BAKERY', 'CHURRAS', 'BBQ', 'SALOON', 'LOUNGE', 'ROOFTOP', 'CANTINA', 'POCHA', 'WINE', 'SPIRITS', 'COCKTAIL', 'BUCCANEERS', 'OCTOPUS', 'SANCTUARY', 'WASTED GRAIN', 'APERITIF', 'OLIVE GARDEN', 'LOS SOMBREROS', 'LOS HORNITOS', 'FILIBERTO', 'RICARDOS', 'MARISCOS', 'LEONCITO', 'DARKSTAR', 'CASA TEMPE', 'ROSETTA', 'PEDAL HAUS', 'HUNDRED MILE', 'BEVERLY ON MAIN', 'DEVILS HIDEAWAY', 'PARK BAR', 'PHILIPPE', 'EVERETT & JONES', 'AVENIDA BRAZIL', 'BONITA BONITA', 'GALPAO GAUCHO', 'HANSHIN', 'KOKEE TEA', 'LLAOLLAO', 'PHO HOA', 'IPPUDO', 'TONG YANG', 'CIBO', 'MODERN SHANGHAI', 'NIKKEI', 'TIONG BAHRU', 'SALAD STOP', 'HARLAN COFFEE', 'YES PLEASE', 'MIDAS CAFE', 'DRUNKEN TIGER', 'POOL BAR', 'ENTERTAINMENT BAR', 'MAD MONKEY', 'BUDDY BEER', 'HOBS', 'RED ROOSTER', 'T BONE', 'BOLD MIS CARNES', 'FAUNA FLORA', 'CAPRICCIOSA', 'CULTURA DO HAMBUGUER', 'SARDINHA', 'POTATO PROJECT', 'COPACABANA', 'ONNOS']):
       return 'Dining'
   
   # Ride Share/Transportation
   if any(x in merchant_upper for x in ['GRAB', 'UBER', 'LYFT', 'TAXI', 'GETTRANSFER', 'ALAMO TOLL', 'DOLLAR RAC', 'PRICELN', 'EASIRENT', 'ADO WEB', 'MUVON']):
       return 'Transportation'
   
   # Car Wash
   if 'CAR WASH' in merchant_upper:
       return 'Car Wash'
   
   # Gas Stations
   if any(x in merchant_upper for x in ['SHELL', 'CHEVRON', 'EXXON', 'MOBIL', 'BP', 'ARCO', '76', 'VALERO', 'CITGO', 'SUNOCO']):
       return 'Gas Station'
   
   # Grocery/Retail
   if any(x in merchant_upper for x in ['GROCERY', 'MARKET', 'SAFEWAY', 'KROGER', 'WHOLE FOODS', 'TRADER JOE', 'COSTCO', 'WALMART', 'WAL-MART', 'TARGET', 'WALGREENS', 'CVS', 'DOLLAR TREE', 'ALBERT HEIJN', '365 MARKET', 'FAMILYMART', 'SEVEN-ELEVEN', '7 11', 'COTTAGE MARKET']):
       return 'Grocery/Retail'
   
   # Shopping
   if any(x in merchant_upper for x in ['UNIQLO', 'MUJI', 'TOYS R US', 'POP MART', 'BRANDSMART', 'LENSDIRECT', 'DONKI', 'KING POWER', 'DUFRY', 'UPS STORE', 'MANGO LISBOA']):
       return 'Shopping'
   
   # Insurance
   if 'INSURANCE' in merchant_upper or 'STATE FARM' in merchant_upper:
       return 'Insurance'
   
   # Subscriptions/Digital
   if any(x in merchant_upper for x in ['GOOGLE', 'BETMGM', 'ACTIVE N FIT', 'EDREAMS']):
       return 'Subscriptions'
   
   # Entertainment/Sports
   if any(x in merchant_upper for x in ['CASINO', 'LEVY', 'CONCESSIONS', 'STADIUM', 'IRONMAN', 'DBACKS', 'ARMK', 'PHANTOM RANCH', 'GRAND CYN', 'ZONA BICI', 'MEDIO AMBIENTE', 'MANSION SPORTS', 'LOOKOUT']):
       return 'Entertainment'
   
   # Health/Personal Care
   if any(x in merchant_upper for x in ['DENTAL', 'SUPERCUTS', 'DENOVA', 'GYM', 'FITNESS', 'YOGA']):
       return 'Health/Personal Care'
   
   # Government/Fees
   if any(x in merchant_upper for x in ['TX.GOV', 'TEXAS.GOV', 'BEXAR', 'FLORISTS']):
       return 'Government/Fees'
   
   # International Services
   if any(x in merchant_upper for x in ['QUIK PLATFORMS', 'JELIZ GLOBAL', 'INFRANOVA', 'MOVE IT']):
       return 'Services'
   
   return 'Other'

def calculate_points(amount, category):
   if amount < 0:
       return 0, 'N/A'
   
   if category in ['Dining', 'Fast Food']:
       return int(round(amount * 3)), 'Chase UR'
   elif category == 'Transportation':
       return int(round(amount * 2)), 'Chase UR'
   else:
       return int(round(amount * 1)), 'Chase UR'

def extract_summary_info(text):
   summary = {}
   
   fees_match = re.search(r'Total fees charged in \d{4}\$?([\d,]+\.[\d]{2})', text)
   if fees_match:
       summary['total_fees'] = float(fees_match.group(1).replace(',', ''))
   
   interest_match = re.search(r'Total interest charged in \d{4}\$?([\d,]+\.[\d]{2})', text)
   if interest_match:
       summary['total_interest'] = float(interest_match.group(1).replace(',', ''))
   
   points_match = re.search(r'Total points available for\s+redemption([\d,]+)', text)
   if points_match:
       summary['rewards_points'] = int(points_match.group(1).replace(',', ''))
   
   return summary

def create_csv_content(data):
   output = io.StringIO()
   writer = csv.writer(output)
   
   writer.writerow(['=== ACCOUNT INFORMATION ==='])
   for key, value in data['account_info'].items():
       writer.writerow([key.replace('_', ' ').title(), value])
   
   writer.writerow([])
   
   if data['summary']:
       writer.writerow(['=== SUMMARY INFORMATION ==='])
       for key, value in data['summary'].items():
           writer.writerow([key.replace('_', ' ').title(), value])
       writer.writerow([])
   
   writer.writerow(['=== TRANSACTIONS ==='])
   writer.writerow([
       'Date', 'Merchant', 'Amount', 'Type', 'Category',
       'Card Type', 'Points Earned', 'Points Program',
       'Foreign Amount', 'Exchange Rate', 'Currency'
   ])
   
   for transaction in data['transactions']:
       writer.writerow([
           transaction['date'],
           transaction['merchant'],
           f"{transaction['amount']:.2f}",
           transaction['type'],
           transaction['category'],
           transaction.get('card_type', ''),
           transaction.get('points_earned', 0),
           transaction.get('points_program', ''),
           transaction.get('foreign_amount', ''),
           transaction.get('exchange_rate', ''),
           transaction.get('currency', '')
       ])
   
   csv_content = output.getvalue()
   output.close()
   
   return csv_content