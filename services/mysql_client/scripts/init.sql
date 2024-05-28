USE gh_api;

CREATE TABLE IF NOT EXISTS languages (
    language_name VARCHAR(50) PRIMARY KEY,
    popularity_count INT,
    tdd_count INT,
    tdd_and_devops_count INT
);