# PhraseBase

# Requirements
- Download dataset from https://www.kaggle.com/rushabhmishra/phrasebase
- Stanford CoreNLP 2018-2-27
- Hadoop 3.0

# Steps
- Clone the repository
- run in terminal
  ``` hadoop com.sun.tools.javac.Main PhraseBase.java ```
- Run
  ``` jar cf PhraseBase.jar PhraseBase*.class ```
- Create input.txt and write any simple sentence with a subject - verb phrase - object eg. Sachin Tendulkar was born in Mumbai.
- Run
  ``` hadoop jar PhraseBase.jar PhraseBase input.txt ```
