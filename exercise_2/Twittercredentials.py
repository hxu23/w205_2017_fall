import tweepy

consumer_key = "dU8wK7jFUSNEzaprvDDe6EAuI";
#eg: consumer_key = "YisfFjiodKtojtUvW4MSEcPm";


consumer_secret = "DGZ0n2X5uoBD0yVQb5nedsDlfMORvgBtTFTmAdmvOOjrbjIlsP";
#eg: consumer_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token = "3013773773-LfrtQ9b1I4t87M41xHnq1j7ldvOP1JNl7zNG5EN";
#eg: access_token = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";

access_token_secret = "VY2QZqSAuyS1ASuwAYpDpz3oRpeZm7PpeW7Zl1st32xBe";
#eg: access_token_secret = "YisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPmYisfFjiodKtojtUvW4MSEcPm";


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)



