import simplejson as json
class config_stopwords:
    data = None
    def __init__(self):
        self.data = json.load(open('config_stopwords.json'))

    def read(self):
        return(self.data['list_stopwords'])

    def remove(self,value):
       tmp = self.data['list_stopwords']
       tmp.remove(value)
       self.data['list_stopwords'] = sorted(list(set(tmp)))

       with open("config_stopwords.json", "w") as jsonFile:
          json.dump(self.data, jsonFile)

    def set(self,value):
       tmp = self.data['list_stopwords']
       tmp.append(value.lower())
       self.data['list_stopwords'] = sorted(list(set(tmp)))

       with open("config_stopwords.json", "w") as jsonFile:
          json.dump(self.data, jsonFile)

