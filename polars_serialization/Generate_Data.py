from mimesis import Person, Address, Datetime
from mimesis.enums import Gender
from icecream import ic
import random
import sys
from humanfriendly import format_size
import polars as pl
person = Person("en")

person = Person()
addess = Address()
datetime = Datetime()


def create_dataset(num=1):
    output = [
        {
            "full_name": person.full_name(gender=Gender.FEMALE),
            "address": addess.address(),
            "name": person.name(),
            "email": person.email(),
            "city": addess.city(),
            "state": addess.state(),
            "birthday": person.birthdate(),
            "degree": person.academic_degree(),
            "university": person.university(),
            "randomdata": random.randint(1000, 2000),
        }
        for x in range(num)
    ]
    return output


dataset = create_dataset(100000)


# ic(dataset[:4])
polars_dataset = pl.from_dicts(dataset)
# polars_dataset.glimpse()


polars_dataset.write_csv('sample_dataset.csv')
polars_dataset.write_parquet('sample_dataset.parquet')
polars_dataset.write_ipc('sample_dataset.arrow')
# polars_dataset.write_ndjson('sample_dataset.json')

# arrow_dataset = polars_dataset.to_arrow()

ic(format_size(sys.getsizeof(dataset)))
ic(format_size(sys.getsizeof(polars_dataset)))
ic(format_size(polars_dataset.estimated_size()))
# ic(format_size(sys.getsizeof(arrow_dataset)))

csv_dataset = pl.read_csv('sample_dataset.csv')
parquet_dataset = pl.read_parquet('sample_dataset.parquet')
arrow_dataset = pl.read_ipc('sample_dataset.arrow')

ic(format_size(sys.getsizeof(csv_dataset)))
ic(format_size(sys.getsizeof(parquet_dataset)))
ic(format_size(sys.getsizeof(arrow_dataset)))
ic(format_size(csv_dataset.estimated_size()))
ic(format_size(parquet_dataset.estimated_size()))
ic(format_size(arrow_dataset.estimated_size()))

# polars_dataset.estimated_size()
import pickle
with open('sample_dataset.pkl', 'wb') as file:
    pickle.dump(polars_dataset, file)
