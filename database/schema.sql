CREATE TABLE transactions (
   id SERIAL PRIMARY KEY,
   transaction_date DATE NOT NULL,
   merchant VARCHAR(255) NOT NULL,
   amount DECIMAL(10, 2) NOT NULL,
   transaction_type VARCHAR(50),
   category VARCHAR(100),
   card_type VARCHAR(50) NOT NULL,
   points_earned INTEGER,
   points_program VARCHAR(50),
   foreign_amount DECIMAL(10, 2),
   exchange_rate DECIMAL(10, 6),
   currency VARCHAR(10),
   statement_date DATE,
   created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transaction_date ON transactions(transaction_date);
CREATE INDEX idx_card_type ON transactions(card_type);
CREATE INDEX idx_merchant ON transactions(merchant);