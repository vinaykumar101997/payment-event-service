INSERT INTO accounts (id, balance, currency)
VALUES
    ('ACC-1001', 1000.00, 'USD'),
    ('ACC-1002', 500.00, 'USD')
ON CONFLICT (id) DO NOTHING;
