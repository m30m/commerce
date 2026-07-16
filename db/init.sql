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

-- cart_items references both parents with ON DELETE CASCADE: deleting a product
-- (or a user) removes the cart lines pointing at it in the same statement, so
-- the catalog delete path cannot leave orphaned rows behind.
CREATE TABLE cart_items (
    id         SERIAL PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    product_id INTEGER NOT NULL REFERENCES products (id) ON DELETE CASCADE,
    qty        INTEGER NOT NULL DEFAULT 1,
    added_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexed access paths the services rely on. The two cart_items indexes also
-- back the ON DELETE CASCADE lookups above: without an index on the referencing
-- column, every parent delete seq-scans cart_items.
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
