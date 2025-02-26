import pandas as pd
import dagster as dg


# The employee asset has basic information about the employee and which department
# they were in each year.
@dg.asset(group_name='Ingestion')
def employee():
    df = pd.read_csv('data/employee.csv')
    return df


# The salary asset has the compensation information for each employee by year
@dg.asset(group_name='Ingestion')
def salary():
    df = pd.read_csv('data/salary.csv')
    return df


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
