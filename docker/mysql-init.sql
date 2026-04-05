CREATE USER 'cdc_user'@'%' IDENTIFIED BY 'cdc_password';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
GRANT SELECT ON testdb.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;

-- Test tables covering common MySQL types
CREATE TABLE testdb.orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    amount DECIMAL(10,2),
    status ENUM('pending','shipped','delivered') DEFAULT 'pending',
    metadata JSON,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE testdb.customers (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    age TINYINT UNSIGNED,
    balance BIGINT UNSIGNED DEFAULT 0,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
