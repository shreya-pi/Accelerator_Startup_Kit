CREATE PROCEDURE [dbo].[GetInactiveCustomers]
AS
BEGIN
    SET NOCOUNT ON;

    SELECT c.customer_id, c.first_name, c.last_name, MAX(r.rental_date) AS last_rental_date
    FROM customer c
    LEFT JOIN rental r ON c.customer_id = r.customer_id
    GROUP BY c.customer_id, c.first_name, c.last_name
    HAVING MAX(r.rental_date) < DATEADD(MONTH, -6, GETDATE()) OR MAX(r.rental_date) IS NULL;
END;
