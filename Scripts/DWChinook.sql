
CREATE SCHEMA dw;
SET search_path TO dw;

CREATE TABLE Dim_Date (
    DateId SERIAL PRIMARY KEY,
    FullDate DATE NOT NULL,
    YearD INT,
    MonthD INT,
    Dayd INT
);

CREATE TABLE Dim_Customer (
    CustomerId INT PRIMARY KEY,
    FullName VARCHAR(120),
    Company VARCHAR(120),
    Address VARCHAR(200),
    City VARCHAR(100),
    State VARCHAR(50),
    Country VARCHAR(50),
    PostalCode VARCHAR(20),
    Phone VARCHAR(30),
    Email VARCHAR(100)
);

CREATE TABLE Dim_Employee (
    EmployeeId INT PRIMARY KEY,
    FullName VARCHAR(120),
    Title VARCHAR(100),
    City VARCHAR(100),
    State VARCHAR(50),
    Country VARCHAR(50),
    Email VARCHAR(100)
);


CREATE TABLE Dim_Track (
    TrackId INT PRIMARY KEY,
    TrackName VARCHAR(200),
    Album VARCHAR(200),
    Artist VARCHAR(200),
    Genre VARCHAR(100),
    MediaType VARCHAR(100),
    Composer VARCHAR(200),
    Milliseconds INT,
    Bytes INT
);

CREATE TABLE Dim_Invoice (
    InvoiceId INT PRIMARY KEY,
    BillingAddress VARCHAR(200),
    BillingCity VARCHAR(100),
    BillingState VARCHAR(50),
    BillingCountry VARCHAR(50),
    BillingPostalCode VARCHAR(20)
);

CREATE TABLE Fact_Sales (
    FactSalesId SERIAL PRIMARY KEY,
    InvoiceId INT REFERENCES Dim_Invoice(InvoiceId),
    TrackId INT REFERENCES Dim_Track(TrackId),
    CustomerId INT REFERENCES Dim_Customer(CustomerId),
    EmployeeId INT REFERENCES Dim_Employee(EmployeeId),
    DateId INT REFERENCES Dim_Date(DateId),
    Quantity INT,
    UnitPrice NUMERIC(10,2),
    Total NUMERIC(10,2)
);
