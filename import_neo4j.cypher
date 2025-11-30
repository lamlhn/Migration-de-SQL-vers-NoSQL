
-- SCRIPT CYPHER POUR IMPORT NEO4J - VERSION CORRECTE
-- Étape 3: Migration des données Neo4j

-- 1. Nettoyage (optionnel pour les tests)
MATCH (n) DETACH DELETE n;

-- 2. Création des contraintes
CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customerId IS UNIQUE;
CREATE CONSTRAINT film_id IF NOT EXISTS FOR (f:Film) REQUIRE f.filmId IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (cat:Category) REQUIRE cat.categoryId IS UNIQUE;
CREATE CONSTRAINT staff_id IF NOT EXISTS FOR (s:Staff) REQUIRE s.staffId IS UNIQUE;

-- 3. Import des nœuds Customer (STRUCTURE RÉELLE)
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/customers.csv' AS row
CREATE (c:Customer {
    customerId: toInteger(row.`customerId:ID(Customer)`),
    firstName: row.firstName,
    lastName: row.lastName
});

-- 4. Import des nœuds Film (STRUCTURE RÉELLE)
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/films.csv' AS row
CREATE (f:Film {
    filmId: toInteger(row.`filmId:ID(Film)`),
    title: row.title,
    releaseYear: toInteger(row.releaseYear)
});

-- 5. Import des nœuds Category (STRUCTURE RÉELLE)
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/categories.csv' AS row
CREATE (cat:Category {
    categoryId: toInteger(row.`categoryId:ID(Category)`),
    categoryName: row.categoryName
});

-- 6. Import des nœuds Staff (STRUCTURE RÉELLE)
LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/staff.csv' AS row
CREATE (s:Staff {
    staffId: toInteger(row.`staffId:ID(Staff)`),
    firstName: row.firstName,
    lastName: row.lastName,
    storeId: toInteger(row.storeId)
});

-- 7. Création des relations WATCHED (VERSION CORRECTE)
:auto
LOAD CSV FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/watched_relations.csv' AS line
WITH line WHERE line[0] <> ":START_ID(Customer)"
CALL {
  WITH line
  WITH toInteger(line[0]) as customerId, toInteger(line[1]) as filmId, line[2] as rentalDate
  MATCH (c:Customer {customerId: customerId})
  MATCH (f:Film {filmId: filmId})
  CREATE (c)-[:WATCHED {rentalDate: datetime(rentalDate)}]->(f)
} IN TRANSACTIONS OF 1000 ROWS
RETURN "Relations WATCHED créées";

-- 8. Création des relations IN_CATEGORY (VERSION CORRECTE)
:auto
LOAD CSV FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/film_category_relations.csv' AS line
WITH line WHERE line[0] <> ":START_ID(Film)"
CALL {
  WITH line
  WITH toInteger(line[0]) as filmId, toInteger(line[1]) as categoryId
  MATCH (f:Film {filmId: filmId})
  MATCH (cat:Category {categoryId: categoryId})
  CREATE (f)-[:IN_CATEGORY]->(cat)
} IN TRANSACTIONS OF 1000 ROWS
RETURN "Relations IN_CATEGORY créées";

-- 9. Validation des imports
MATCH (n) RETURN labels(n) as label, count(*) as count
ORDER BY label;

-- 10. Validation des relations
MATCH ()-[r]->() RETURN type(r) as relation, count(*) as count
ORDER BY relation;
