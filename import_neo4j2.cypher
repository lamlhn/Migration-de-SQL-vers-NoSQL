MATCH (n) DETACH DELETE n;

CREATE CONSTRAINT customer_id IF NOT EXISTS FOR (c:Customer) REQUIRE c.customerId IS UNIQUE;
CREATE CONSTRAINT film_id IF NOT EXISTS FOR (f:Film) REQUIRE f.filmId IS UNIQUE;
CREATE CONSTRAINT category_id IF NOT EXISTS FOR (cat:Category) REQUIRE cat.categoryId IS UNIQUE;
CREATE CONSTRAINT staff_id IF NOT EXISTS FOR (s:Staff) REQUIRE s.staffId IS UNIQUE;

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/customers.csv' AS row
CREATE (c:Customer {
    customerId: toInteger(row.`customerId:ID(Customer)`),
    firstName: row.firstName,
    lastName: row.lastName
});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/films.csv' AS row
CREATE (f:Film {
    filmId: toInteger(row.`filmId:ID(Film)`),
    title: row.title,
    releaseYear: toInteger(row.releaseYear)
});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/categories.csv' AS row
CREATE (cat:Category {
    categoryId: toInteger(row.`categoryId:ID(Category)`),
    categoryName: row.categoryName
});

LOAD CSV WITH HEADERS FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/staff.csv' AS row
CREATE (s:Staff {
    staffId: toInteger(row.`staffId:ID(Staff)`),
    firstName: row.firstName,
    lastName: row.lastName,
    storeId: toInteger(row.storeId)
});

:auto
LOAD CSV FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/watched_relations.csv' AS line
WITH line WHERE line[0] <> ":START_ID(Customer)"
WITH toInteger(line[0]) as customerId, toInteger(line[1]) as filmId, line[2] as rentalDate
MATCH (c:Customer {customerId: customerId})
MATCH (f:Film {filmId: filmId})
CREATE (c)-[:WATCHED {rentalDate: rentalDate}]->(f)
RETURN count(*) as relationships_created;


:auto
LOAD CSV FROM 'https://raw.githubusercontent.com/lamlhn/Migration-de-SQL-vers-NoSQL/refs/heads/main/film_category_relations.csv' AS line
WITH line WHERE line[0] <> ":START_ID(Film)"
WITH toInteger(line[0]) as filmId, toInteger(line[1]) as categoryId
MATCH (f:Film {filmId: filmId})
MATCH (cat:Category {categoryId: categoryId})
CREATE (f)-[:IN_CATEGORY]->(cat)
RETURN count(*) as relationships_created;

MATCH (n) RETURN labels(n) as label, count(*) as count
ORDER BY label;

MATCH ()-[r]->() RETURN type(r) as relation, count(*) as count
ORDER BY relation;
