from celery import Celery

app = Celery()

@app.task
def test():
    print('test')