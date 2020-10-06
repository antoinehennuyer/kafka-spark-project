# Prestacop project

Create a drone service to help the police make parking tickets.

Each drone sends messages regularly, with: drone location, time and drone id.
And if a violation occured, with the two following additional fields: violation code and image id.

When the drone's message violation code indicates that human interaction is required (1% of the time), an alarm must be send to a human operator.

With all the data, PrestaCop wants to make statistics and improve their services. To improve those statistics, NYPD historical data should be used. However, NYPD poses two constraints:  
The computers are old and not very powerful and the data is stored in a large CSV.

# Architecture:

The architecture is split in 5 sections:  
- Read CSV file and pubish it to the Kstream.  
- Simulate sending message from the drone to Kstream.  
- Consume stream messages and stores them in a DataLake each day.  
- Consume stream messages and raises an alarm when human interaction is required.  
- Analyse and make statistics about the data stored in the DataLake.
