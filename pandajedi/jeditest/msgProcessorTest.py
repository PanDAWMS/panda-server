import multiprocessing

from pandajedi.jediorder import JediMsgProcessor

stop_event = multiprocessing.Event()

agent = multiprocessing.Process(target=JediMsgProcessor.launcher, args=(stop_event,))
agent.start()
stop_event.wait()
