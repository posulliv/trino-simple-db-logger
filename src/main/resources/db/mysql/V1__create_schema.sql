CREATE TABLE queries (
  id INT NOT NULL AUTO_INCREMENT,
  query_id VARCHAR(50),
  catalog VARCHAR(256),
  `schema` VARCHAR(256),
  environment VARCHAR(256),
  query_text LONGTEXT NOT NULL,
  query_plan LONGTEXT,
  query_json LONGTEXT,
  created TIMESTAMP(6) NOT NULL,
  finished TIMESTAMP(6) NOT NULL,
  query_state VARCHAR(32) NOT NULL,
  error_info LONGTEXT,
  PRIMARY KEY(id),
  UNIQUE INDEX(query_id),
  INDEX(created, finished)
);