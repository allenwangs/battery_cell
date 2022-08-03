select 2.0 + seq4()/1000 as voltage
from table(generator(rowcount => 1500)) 