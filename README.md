# DS-3002-Project-1
ETL processor using raw tennis csv online

This ETL processor takes in a csv of tennis stats (hosted online so no need to have the file locally).

The first option is to convert the file to csv, json, or a sql db. If one of these options isn't chosen, this question will be re-asked.

Next the user is given the option to add or delete columns or do nothing.

If the user chooses add column, the available columns not in the new table will be show as potential options to add (the user can either add a singular column or multiple columns).

If the user chooses to drop columns, the columns already in the new data set will be presented as potential columns to drop.

Note: if drop columns is chosen before add, there will be no column options to drop as there are no columns in the new data set.

After the user chooses the columns, they will be asked again if the want to do any other operations.

A summary of the table as well as the number of rows and columns is presented after each modification.

Finally the user is asked to export the modified table in csv, json, or db format.
