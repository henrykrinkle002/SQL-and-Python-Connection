--Create Schema CourseWork;
--SET SEARCH_PATH to CourseWork;

CREATE TABLE book (
    bno INT PRIMARY KEY
	CHECK (bno >= 100000 and bno <= 999999),           
    title VARCHAR(255) NOT NULL,     
    author VARCHAR(255) NOT NULL,    
    category VARCHAR(50) 
	CHECK (category IN ('Science', 'Lifestyle', 'Arts', 'Leisure')), 
    price NUMERIC NOT NULL,   
    sales INT DEFAULT 0             
);

CREATE TABLE customer (
    cno INT PRIMARY KEY
	CHECK (cno >= 100000 and cno <= 999999),             
    name VARCHAR(255) NOT NULL,       
    address VARCHAR(255) NOT NULL,   
    balance NUMERIC DEFAULT 0 
);


--I tried using Foreign Key but then I am only able 
--to add the book and customer values 
--but not record the data in bookorder

CREATE TABLE bookOrder (
    cno INT,                         
    bno INT,                         
    orderTime TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  
    qty INT NOT NULL,                     
);



Create Or Replace Function handleorder()    --E)
Returns Trigger AS $$
DECLARE
   bookPrice NUMERIC;
   bookSales INTEGER;
   customerExists Boolean;
BEGIN
   IF NOT EXISTS(Select 1 from Book where bno = NEW.bno)then
      Insert Into Book(bno, title, author, category, price) values
      (New.bno, 'Gulliver Travels', 'Jonathan Swift', 'Leisure', 50.00);
   End IF;

   SELECT price INTO bookPrice
   FROM Book
   WHERE bno = NEW.bno;

   SELECT sales INTO bookSales
   FROM Book
   WHERE bno = NEW.bno;

   IF NOT EXISTS(Select 1 from Customer where cno=NEW.cno) then
      Insert Into Customer(cno, name, address, balance) values
      (NEW.cno, 'ABC', '456 Main St, London', 0.00);
   End IF;
   

   Update customer 
   SET balance = balance - (bookPrice*NEW.qty)
   where cno = NEW.cno;

   Update Book
   SET sales = bookSales + NEW.qty
   where bno = NEW.bno;
   
   Return NEW;
END;
$$ LANGUAGE plpgsql;



CREATE Trigger handle_order_trigger   --F)
AFTER INSERT ON bookOrder
For EACH ROW
EXECUTE Function handleorder();


Create or Replace Function record_payment(customer_Number integer, payment numeric)
RETURNS VOID AS $$
BEGIN
   Update customer 
   SET balance = balance + payment
   WHERE cno = customer_Number;

END;
$$ LANGUAGE plpgsql;

SELECT record_payment(678901, 500);



select record_payment(100001, 100);


Select cno AS Customer_Number,
name AS customer_Name,
address AS customer_address,
balance AS Remaining_Dues
from customer;



   











