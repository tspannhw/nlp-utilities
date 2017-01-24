from textblob import TextBlob
 
b = TextBlob("Spellin iz vaerry haerd to do. I do not like this spelling product at all it is terrible and I am very mad.")
print(b.correct())
print(b.sentiment)
print(b.sentiment.polarity)
