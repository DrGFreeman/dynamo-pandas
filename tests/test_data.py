import math
import random
from string import ascii_letters

import pandas as pd

"""Define a test dataframe with mixed types and missing values:
>>> print(test_df)
      A  B               C          D                         E     F  \
0   abc  2 0 days 19:32:01 2000-01-01 2000-01-01 00:00:00+00:00   128   
1  None  3 1 days 01:33:20 2000-12-31 2000-12-31 23:59:59+00:00  <NA>   
2   NaN  4 2 days 23:06:40        NaT                       NaT  <NA>   

          G  id  
0  3.141593   0  
1       NaN   1  
2       NaN   2  
>>> test_df.info()
<class 'pandas.core.frame.DataFrame'>
RangeIndex: 3 entries, 0 to 2
Data columns (total 8 columns):
 #   Column  Non-Null Count  Dtype              
---  ------  --------------  -----              
 0   A       1 non-null      object             
 1   B       3 non-null      int64              
 2   C       3 non-null      timedelta64[ns]    
 3   D       2 non-null      datetime64[ns]     
 4   E       2 non-null      datetime64[ns, UTC]
 5   F       1 non-null      Int32              
 6   G       1 non-null      float64            
 7   id      3 non-null      int64              
dtypes: Int32(1), datetime64[ns, UTC](1), datetime64[ns](1), float64(1), int64(2), object(1), timedelta64[ns](1)
memory usage: 311.0+ bytes"""  # noqa: E501, W291
test_df = pd.DataFrame(
    [
        {
            "A": "abc",
            "B": 2,
            "C": 70321.4,
            "D": "2000-01-01",
            "E": "2000-01-01",
            "F": 128,
            "G": math.pi,
        },
        {
            "A": None,
            "B": 3,
            "C": 92000,
            "D": "2000-12-31",
            "E": "2000-12-31 23:59:59",
            "F": None,
            "G": None,
        },
        {"B": 4, "C": 256000},
    ]
).astype(
    {
        "C": "timedelta64[s]",
        "D": "datetime64[ns]",
        "E": "datetime64[ns, UTC]",
        "F": "Int32",
    }
)
test_df["id"] = range(len(test_df))

large_table_items = [
    dict(id=i, letter=random.choice(ascii_letters), number=random.randint(0, 1000))
    for i in range(250)
]
