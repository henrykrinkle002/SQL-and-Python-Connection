
INSERT INTO book (bno, title, author, category, price) -- A)
VALUES (123456, 'Python Programming', 'John Doe', 'Science', 29.99);

DELETE FROM book   --B)
WHERE bno = 123456;

INSERT INTO customer (cno, name, address, balance)     --C)
VALUES (123456, 'Alice Smith', '123 Main St, London', 0.00);

DELETE FROM customer     --D)
WHERE cno = 123456;


INSERT INTO bookOrder (cno, bno, qty)  --E)
VALUES (678901, 123456, 10);



Create or Replace Function record_payment(customer_Number integer, payment numeric)
RETURNS VOID AS $$
BEGIN
   Update customer 
   SET balance = balance + payment
   WHERE cno = customer_Number;

END;
$$ LANGUAGE plpgsql;

select record_payment(678901, 500) --F)

select cno as Customer_Number, 
name as Customer_Name, 
address, 
balance as Remaining_Payment from customer;



SELECT                    --G)
    b.title as Book_Title, 
    c.name as customer_name, 
    c.address
FROM 
    customer c JOIN bookOrder bo 
ON c.cno = bo.cno 

JOIN 
    book b ON bo.bno = b.bno
WHERE 
    b.title LIKE '%Python%' 
ORDER BY 
    b.title, 
    c.name;

INSERT INTO Book (bno, title, author, category, price, sales)
VALUES (133456, 'Learn Python Basics', 'John Doe', 'Science', 50.00, 5);


INSERT INTO BookOrder (cno, bno, qty)
VALUES (678901, 133456, 2);
insert into BookOrder (cno, bno, qty) values
(688901, 133456, 1);


SELECT                           --H)
    c.name AS customer_name,
    b.bno AS book_number,
    b.title AS book_title,
    b.author AS book_author
FROM 
    customer c
JOIN 
    bookOrder bo ON c.cno = bo.cno
JOIN 
    book b ON bo.bno = b.bno
WHERE 
    c.cno = 678901  
ORDER BY 
    b.bno;  



Select category,            --I)
Sum(sales) AS total_books_sold,
Sum(sales * price) AS total_sales_value
FROM book
Group By category
Order By category;


SELECT                            --J)
    c.cno AS customer_number,
    c.name AS customer_name,
    SUM(bo.qty) AS total_copies_on_order
FROM 
    customer c
JOIN 
    bookOrder bo ON c.cno = bo.cno
GROUP BY 
    c.cno, c.name
ORDER BY 
    c.cno;
















