from dateutil import parser

value = '30/12/2020'
value = parser.parse(value,dayfirst=True).date()

print(value.month)

valueInFact = '30/12/2020'
valueInFact = parser.parse(valueInFact,dayfirst=True).replace(hour=0, minute=0, second=0, microsecond=0)

print(valueInFact.month)