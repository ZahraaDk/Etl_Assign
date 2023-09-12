-- get rental duration function
-- DROP FUNCTION IF EXISTS get_rental_duration;
-- we can remove the drop bc we are replacing it, this is an etl that will run every 5s, we dont want to drop and create every 5 sec
-- Hence, we can just update it (replace)
CREATE OR REPLACE FUNCTION dw_reporting.get_rental_duration(rental_date TIMESTAMP, return_date TIMESTAMP)
RETURN INTEGER AS $$
BEGIN 
    RETURN EXTRACT(AGE from (return_date - rental_date));
END;
$$LANGUAGE plpgsql;

-- get film category name 
-- DROP FUNCTION IF EXISTS get_film_category
CREATE OR REPLACE FUNCTION dw_reporting.get_film_category(film_id INTEGER )
RETURNS TEXT AS $$
DECLARE 
    category_name TEXT;
BEGIN
    SELECT 
        categ.name INTO category_name
    FROM public.film_category AS film_category
    INNER JOIN public.category AS categ 
    ON categ.category_id = film_category.category_id
    WHERE film_category.film_id = film_id
    LIMIT 1; 
    RETURN category_name
END;
$$ LANGUAGE plpgsql
