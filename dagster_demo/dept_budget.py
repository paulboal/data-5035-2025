import pandas as pd
import dagster as dg

# Add pytest so we can setup tests
import pytest


# The employee asset has basic information about the employee and which department
# they were in each year.
@dg.asset(group_name='Ingestion')
def employee():
    df = pd.read_csv('data/employee.csv')
    return df

## Verify the employee information
## 1. Expected number of records
## 2. Year is numeric
## 3. Year Range is 2000-2099
## 4. Employee is numeric

@pytest.fixture
def test_employee():
    return employee()

def test_employee_count(test_employee):
    assert(len(test_employee) == 12)

def test_year_isnumeric(test_employee):
    assert(test_employee['year'].dtype == int)

def test_year_range(test_employee):
    assert((test_employee['year'] >= 2000).all() and (test_employee['year'] < 2100).all())

def test_empid_isnumeric(test_employee):
    assert(test_employee['employee_id'].dtype == int)


# The salary asset has the compensation information for each employee by year
@dg.asset(group_name='Ingestion')
def salary():
    df = pd.read_csv('data/salary.csv')
    return df

## Test salary data
## 1. Test year is numeric
## 2. Test year is 2000-2099

@pytest.fixture()

def test_year_range():
    return salary()


# Finally, we have the department salary asset that summarizes salary information by department
@dg.asset(group_name='Transformation')
def summary(employee: pd.DataFrame, salary: pd.DataFrame):
    df = pd.DataFrame(pd.merge(
        salary, 
        employee, 
        on = ['employee_id','year']).groupby(['department','year'])['salary'].sum())
    df.to_csv('output/summary.csv')
    



# Combine everything into the dag
defs = dg.Definitions(
  assets=[salary,
      employee,
      summary
  ]
)
