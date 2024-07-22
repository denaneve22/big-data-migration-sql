-- Crear la base de datos
CREATE DATABASE [big-data-migration];
GO

-- Usar la base de datos
USE [big-data-migration];
GO

-- Crear la tabla departments
CREATE TABLE departments (
    id INT PRIMARY KEY NOT NULL,
    department NVARCHAR(255) NOT NULL
);
GO

--Insertamos valores dummy
INSERT INTO departments (id, department)
VALUES 
    (-1, 'Unknown Department');

-- Crear la tabla jobs
CREATE TABLE jobs (
    id INT PRIMARY KEY NOT NULL,
    job NVARCHAR(255) NOT NULL
);
GO

--Insertamos valores dummy
INSERT INTO jobs (id, job)
VALUES 
    (-1, 'Unknown Job');

-- Crear la tabla employees
CREATE TABLE employees (
    id INT,
    name NVARCHAR(255),
    datetime NVARCHAR(255),
    department_id INT,
    job_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id)
);
GO
