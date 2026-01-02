CREATE TABLE entities (
 entity_id INT PRIMARY KEY AUTO_INCREMENT,
 entity_name VARCHAR(150) NOT NULL,
 entity_type VARCHAR(30),
 registration_number VARCHAR(50),
 incorporation_date DATE,
 country_code VARCHAR(3),
 state_code VARCHAR(50),
 status VARCHAR(30),
 industry VARCHAR(100),
 contact_email VARCHAR(100),
 last_update DATE,
 created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
