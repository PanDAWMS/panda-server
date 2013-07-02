# calculate priority for user jobs
def calculatePriority(priorityOffset,serNum,weight):
    priority = 1000 + priorityOffset - (serNum / 5) - int(100 * weight)
    return priority
