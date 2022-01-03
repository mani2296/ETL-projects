from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator

default_args={
    'start_date':datetime(2022,1,1)
    }
with DAG('Batch1',schedule_interval='0 \1 * * *',default_args=default_args,catchup=False) as dag:
    
    
    delete_dir = BashOperator(
        task_id="delete_dir1",
        bash_command="hdfs dfs -rm -r -f /user/hduser/expense_af"
        )

    imp_cmd="""
    sqoop import --connect jdbc:mysql://localhost/tempdb --username root --password-file /user/hduser/passfile/passwordfile1 --table tblexpense --target-dir /user/hduser/expense_af \
    --incremental append --check-column id --last-value 0 -m1 --driver com.mysql.cj.jdbc.Driver
    """

    sqoop_imp=BashOperator(
        task_id='sqoop_imp',
        bash_command=imp_cmd
        )
    
    check_folder_exist=BashOperator(
        task_id='check_folder_exist',
        bash_command='hdfs dfs -test -d /user/hduser/expense_af'
        )
    
    hive_con=BashOperator(
        task_id='hive_con',
        bash_command='hive -f /home/hduser/exp_af.hql'
        )
    
    exp_cmd="""
    sqoop export --connect jdbc:mysql://localhost/tempdb --username root --password-file /user/hduser/passfile/passwordfile1 --table expense_summary --columns "expense_date,total_amount" \
    --export-dir /user/hive/warehouse/expense_curated.db/exp_summary_af --update-mode allowinsert --update-key expense_date -m1
    """
    
    sqoop_import=BashOperator(
        task_id='sqoop_export',
        bash_command=exp_cmd
        )
    
    delete_dir >> sqoop_imp >> check_folder_exist >> hive_con >> sqoop_import
    