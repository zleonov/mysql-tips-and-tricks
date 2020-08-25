DROP DATABASE IF EXISTS company;
CREATE DATABASE IF NOT EXISTS company;
USE company;

DROP TABLE IF EXISTS departments;
CREATE TABLE IF NOT EXISTS departments (
  id int(11) NOT NULL AUTO_INCREMENT,
  dept_name varchar(40) NOT NULL,
  create_date TIMESTAMP NULL DEFAULT NULL,
  last_update_date TIMESTAMP NULL DEFAULT NULL,  
  PRIMARY KEY (id),
  UNIQUE KEY dept_name (dept_name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS employees;
CREATE TABLE IF NOT EXISTS employees (
  id int(11) NOT NULL AUTO_INCREMENT,
  first_name varchar(14) NOT NULL,
  last_name varchar(16) NOT NULL,
  birth_date date NOT NULL,
  gender enum('M','F') NOT NULL,
  hire_date date NOT NULL,
  create_date TIMESTAMP NULL DEFAULT NULL,
  last_update_date TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY first_name_last_name (first_name,last_name)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS dept_emp;
CREATE TABLE IF NOT EXISTS dept_emp (
  id int(11) NOT NULL AUTO_INCREMENT,
  dept_id int(11) NOT NULL,
  emp_id int(11) NOT NULL,
  from_date date NOT NULL,
  to_date date NOT NULL,
  create_date TIMESTAMP NULL DEFAULT NULL,
  last_update_date TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY employee_id_department_id (emp_id,dept_id),
  KEY FK_dept_emp_departments (dept_id),
  CONSTRAINT FK_dept_emp_departments FOREIGN KEY (dept_id) REFERENCES departments (id),
  CONSTRAINT FK_dept_emp_employees FOREIGN KEY (emp_id) REFERENCES employees (id)
) ENGINE=InnoDB;

DROP TABLE IF EXISTS emp_salary;
CREATE TABLE IF NOT EXISTS emp_salary (
  id int(11) NOT NULL AUTO_INCREMENT,
  emp_id int(11) NOT NULL,
  salary int(11) NOT NULL,
  from_date date NOT NULL,
  to_date date NOT NULL,
  create_date TIMESTAMP NULL DEFAULT NULL,
  last_update_date TIMESTAMP NULL DEFAULT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY emp_id_from_date (emp_id,from_date),
  CONSTRAINT FK_emp_salary_employees FOREIGN KEY (emp_id) REFERENCES employees (id)
) ENGINE=InnoDB;