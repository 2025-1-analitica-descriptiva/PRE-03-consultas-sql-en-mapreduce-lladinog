"""Taller evaluable"""

# pylint: disable=broad-exception-raised
# pylint: disable=import-error

from homework.mapreduce import run_mapreduce_job as run_mapreduce_job

#
# COLUMNS
# total_bill, tip, sex, smoker, day, time, size
#

# SELECT *, tip/total_bill AS tip_rate FROM tips;

def mapper_query_1(sequence):
    """Mapper para la consulta 1"""
    result= []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()+ ",tip_rate"))
        else:
            row_values = row.strip().split(",")
            total_bill = float(row_values[0])
            tip = float(row_values[1])
            tip_rate = tip / total_bill
            result.append((index, row.strip() + "," + str(tip_rate)))
    return result

def reducer_query_1(sequence):
    """Reducer para la consulta 1"""
    return sequence

# SELECT * FROM tips WHERE time = 'Dinner';

def mapper_query_2(sequence):
    """Mapper para la consulta 2"""
    result= []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            time = row_values[5]
            if time == "Dinner":
                result.append((index, row.strip()))
    return result

def reducer_query_2(sequence):
    """Reducer para la consulta 2"""
    return sequence

# SELECT * FROM tips WHERE time = 'Dinner AND tip > 5.00;

def mapper_query_3(sequence):
    """Mapper para la consulta 3"""
    result= []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            time = row_values[5]
            tip = float(row_values[1])
            if time == "Dinner" and tip > 5.00:
                result.append((index, row.strip()))
    return result

def reducer_query_3(sequence):
    """Reducer para la consulta 3"""
    return sequence

# SELECT * FROM tips WHERE size >= 5 OR total_bill > 45;

def mapper_query_4(sequence):
    """Mapper para la consulta 4"""
    result= []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            result.append((index, row.strip()))
        else:
            row_values = row.strip().split(",")
            size = int(row_values[6])
            total_bill = float(row_values[0])
            if size >= 5 or total_bill > 45:
                result.append((index, row.strip()))
    return result

def reducer_query_4(sequence):
    """Reducer para la consulta 4"""
    return sequence

# SELECT sex, count(*) FROM tips GROUP BY sex;

def mapper_query_5(sequence):
    """Mapper para la consulta 5"""
    result= []
    for index, (_, row) in enumerate(sequence):
        if index == 0:
            continue;
        row_values = row.strip().split(",")
        result.append((row_values[2], 1))
    return result

def reducer_query_5(sequence):
    """Reducer para la consulta 5"""
    counter = dict()
    for key, value in sequence:
        if key not in counter:
            counter[key] = 0
        counter[key] += value
    return list(counter.items())
    

#
# ORQUESTADOR:
#
def run():
    """Orquestador"""
    run_mapreduce_job(
        mapper = mapper_query_1,
        reducer = reducer_query_1,
        input_directory="files/input",
        output_directory="files/query_1",
    )

    run_mapreduce_job(
        mapper = mapper_query_2,
        reducer = reducer_query_2,
        input_directory="files/input",
        output_directory="files/query_2",
    )

    run_mapreduce_job(
        mapper = mapper_query_3,
        reducer = reducer_query_3,
        input_directory="files/input",
        output_directory="files/query_3",
    )

    run_mapreduce_job(
        mapper = mapper_query_4,
        reducer = reducer_query_4,
        input_directory="files/input",
        output_directory="files/query_4",
    )
    
    run_mapreduce_job(
        mapper = mapper_query_5,
        reducer = reducer_query_5,
        input_directory="files/input",
        output_directory="files/query_5",
    )
    
if __name__ == "__main__":

    run()
