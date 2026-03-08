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

s3 = boto3.client('s3')
textract = boto3.client('textract')

DB_HOST = os.environ['DB_HOST']
DB_NAME = os.environ['DB_NAME']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_PORT = int(os.environ.get('DB_PORT', '5432'))

def lambda_handler(event, context):
   bucket = event['Records'][0]['s3']['bucket']['name']
   key = unquote_plus(event['Records'][0]['s3']['object']['key'])
   
   print(f"Processing file: {key} from bucket: {bucket}")
   
   # Extract statement date from filename
   filename = key.split('/')[-1]
   stmt_date = f"{filename[:4]}-{filename[4:6]}-{filename[6:8]}"
   print(f"Statement date: {stmt_date}")
   
   # Start Textract job
   response = textract.start_document_text_detection(
       DocumentLocation={'S3Object': {'Bucket': bucket, 'Name': key}}
   )
   
   job_id = response['JobId']
   print(f"Started Textract job: {job_id}")
   
   # Poll for completion
   max_attempts = 60
   attempt = 0
   
   while attempt < max_attempts:
       result = textract.get_document_text_detection(JobId=job_id)
       status = result['JobStatus']
       
       if status == 'SUCCEEDED':
           break
       elif status == 'FAILED':
           raise Exception(f"Textract job failed: {job_id}")
       
       time.sleep(2)
       attempt += 1
   
   if attempt >= max_attempts:
       raise Exception(f"Textract job timed out: {job_id}")
   
   # Extract text
   text_lines = []
   next_token = None
   
   while True:
       if next_token:
           result = textract.get_document_text_detection(JobId=job_id, NextToken=next_token)
       
       for block in result['Blocks']:
           if block['BlockType'] == 'LINE':
               text_lines.append(block['Text'])
       
       next_token = result.get('NextToken')
       if not next_token:
           break
   
   # Save debug file
   debug_key = key.replace('chase-hyatt/', 'Processed/debug-').replace('.pdf', '.txt')
   s3.put_object(Bucket=bucket, Key=debug_key, Body='\n'.join(text_lines))
   
   # Parse transactions
   extracted_data = parse_chase_hyatt_statement(text_lines)
   
   # Create CSV
   csv_buffer = io.StringIO()
   csv_writer = csv.writer(csv_buffer)
   
   csv_writer.writerow([
       'Date', 'Merchant', 'Amount', 'Type', 'Category',
       'Card Type', 'Points Earned', 'Points Program',
       'Foreign Amount', 'Exchange Rate', 'Currency'
   ])
   
   for txn in extracted_data['transactions']:
       csv_writer.writerow([
           txn['date'], txn['merchant'], txn['amount'], txn['type'],
           txn['category'], 'Chase World of Hyatt', txn['points_earned'],
           txn['points_program'], txn.get('foreign_amount', ''),
           txn.get('exchange_rate', ''), txn.get('currency', 'USD')
       ])
   
   # Upload CSV
   output_key = key.replace('chase-hyatt/', 'Processed/').replace('.pdf', '.csv')
   s3.put_object(Bucket=bucket, Key=output_key, Body=csv_buffer.getvalue())
   
   # Insert into database
   insert_transactions(extracted_data['transactions'], 'Chase World of Hyatt', stmt_date)
   
   return {
       'statusCode': 200,
       'body': json.dumps({
           'message': 'Processing complete',
           'transactions': len(extracted_data['transactions']),
           'output_file': output_key
       })
   }

def insert_transactions(transactions, card_type, stmt_date):
   conn = pg8000.Connection(
       host=DB_HOST, database=DB_NAME,
       user=DB_USER, password=DB_PASSWORD, port=DB_PORT
   )
   
   # Delete existing transactions for this statement
   conn.run("DELETE FROM transactions WHERE card_type = :ct AND statement_date = :sd",
            ct=card_type, sd=stmt_date)
   
   for txn in transactions:
       date_str = txn['date']
       if len(date_str) == 5:
           date_str = f"2025-{date_str[:2]}-{date_str[3:]}"
       
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

def parse_chase_hyatt_statement(lines):
   transactions = []
   i = 0
   
   while i < len(lines):
       line = lines[i].strip()
       date_match = re.match(r'^(\d{2}/\d{2})$', line)
       
       if date_match and i + 2 < len(lines):
           date_str = date_match.group(1)
           merchant = lines[i + 1].strip()
           amount_line = lines[i + 2].strip()
           
           amount_match = re.search(r'([\d,]+\.\d{2})', amount_line)
           if amount_match:
               amount = float(amount_match.group(1).replace(',', ''))
               txn_type = 'Credit' if 'PAYMENT' in merchant.upper() else 'Debit'
               
               foreign_amount = None
               exchange_rate = None
               currency = 'USD'
               
               php_match = re.search(r'([\d,]+\.\d{2})\s*PHP', amount_line)
               if php_match:
                   foreign_amount = float(php_match.group(1).replace(',', ''))
                   currency = 'PHP'
                   exchange_rate = round(foreign_amount / amount, 6) if amount > 0 else None
               
               category = categorize_merchant(merchant)
               points_earned, points_program = calculate_hyatt_points(merchant, amount, category)
               
               transactions.append({
                   'date': date_str, 'merchant': merchant, 'amount': amount,
                   'type': txn_type, 'category': category,
                   'points_earned': points_earned, 'points_program': 'World of Hyatt',
                   'foreign_amount': foreign_amount, 'exchange_rate': exchange_rate,
                   'currency': currency
               })
               
               i += 3
               continue
       
       i += 1
   
   return {'transactions': transactions}

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
   
   # Hyatt Properties (separate for 4x points)
   if 'HYATT' in merchant_upper:
       return 'Hyatt Property'
   
   # Hotels/Lodging (other hotels)
   if any(x in merchant_upper for x in ['MARRIOTT', 'COURTYARD', 'SHERATON', 'HILTON', 'HOTEL', 'UNDER CANVAS', 'VENETIAN', 'PALAZZO', 'TIKI TIKI RESORTS', 'IM HOTEL', 'HOTWIRE', 'BOOKING.COM', 'TRIP.COM']):
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

def calculate_hyatt_points(merchant, amount, category):
   """
   Chase World of Hyatt card earning structure:
   - 4x points on Hyatt purchases
   - 2x points on dining, airfare, local transit, and fitness club memberships
   - 1x points on all other purchases
   """
   if amount < 0:  # Credits/payments don't earn points
       return 0, 'N/A'
   
   base_points = int(amount)
   
   # 4x on Hyatt stays
   if category == 'Hyatt Property':
       return base_points * 4, 'World of Hyatt'
   # 2x on dining, airfare, transit, fitness
   elif category in ['Dining', 'Fast Food', 'Airlines', 'Transportation', 'Health/Personal Care']:
       return base_points * 2, 'World of Hyatt'
   # 1x on everything else
   else:
       return base_points, 'World of Hyatt'
