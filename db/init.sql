-- Schema + seed data for the eyebench commerce system.
-- Loaded automatically by the Postgres container on first start.

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    price       NUMERIC(10, 2) NOT NULL,
    description TEXT NOT NULL,
    category    TEXT NOT NULL
);

CREATE TABLE users (
    id         SERIAL PRIMARY KEY,
    email      TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE cart_items (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL,
    product_id INTEGER NOT NULL,
    qty        INTEGER NOT NULL DEFAULT 1,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexed access paths the services rely on.
CREATE INDEX idx_cart_items_user ON cart_items (user_id);
CREATE INDEX idx_cart_items_product ON cart_items (product_id);
CREATE INDEX idx_products_category ON products (category);

-- ---------------------------------------------------------------------------
-- Seed: 1000 products across 5 categories.
INSERT INTO products (name, price, description, category)
SELECT
    'Product ' || g,
    round((random() * 200 + 1)::numeric, 2),
    'A fine product number ' || g || ' for all your needs.',
    (ARRAY['electronics', 'books', 'home', 'toys', 'clothing'])[1 + floor(random() * 5)::int]
FROM generate_series(1, 1000) AS g;

-- Seed: 500 users.
INSERT INTO users (email)
SELECT 'user' || g || '@example.com'
FROM generate_series(1, 500) AS g;

-- Seed: ~3000 cart items, biased toward low-numbered products so popularity
-- has a realistic skew.
INSERT INTO cart_items (user_id, product_id, qty)
SELECT
    1 + floor(random() * 500)::int,
    1 + floor(power(random(), 3) * 999)::int,
    1 + floor(random() * 4)::int
FROM generate_series(1, 3000);

ANALYZE;
