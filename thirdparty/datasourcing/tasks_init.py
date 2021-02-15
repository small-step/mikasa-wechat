import logging
import datasourcing

logging.basicConfig(filename='run.log',level=logging.INFO)

'''
注意init系列函数不能同时执行两个相同任务，否则会出现重复数据
'''

if __name__ == "__main__":
    graph = datasourcing.workflow.TaskGraph('data_init_task')
    init_stock_basic = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='init_stock_basic',
                python_callable=datasourcing.stock.init_stock_basic,
                op_args=[],
                op_kwargs={})

    init_trade_cal = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='init_trade_cal',
                python_callable=datasourcing.stock.init_trade_cal,
                op_args=[],
                op_kwargs={})

    init_stock_daily = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='init_stock_daily',
                python_callable=datasourcing.stock.init_stock_daily,
                op_args=[],
                op_kwargs={})

    init_stock_weekly = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='init_stock_weekly',
                python_callable=datasourcing.stock.init_stock_weekly,
                op_args=[],
                op_kwargs={})

    init_stock_monthly = datasourcing.workflow.PythonTask(
                graph=graph,
                task_id='init_stock_monthly',
                python_callable=datasourcing.stock.init_stock_monthly,
                op_args=[],
                op_kwargs={})

    init_trade_cal >> init_stock_basic
    init_stock_basic >> [init_stock_daily, init_stock_monthly, init_stock_weekly]
    graph.run()