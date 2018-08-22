# Phrase Sense Disambiguation for NLP using PhraseBase

# Requirements
- Download datasets from https://www.kaggle.com/rushabhmishra/phrasebase
- Stanford CoreNLP 2018-2-27
- Hadoop 3.0

# Steps
- Clone the repository
- Run in terminal
  ``` hadoop com.sun.tools.javac.Main PhraseBase.java ```
- Run
  ``` jar cf PhraseBase.jar PhraseBase*.class ```
- Create input.txt and write any simple sentence that has
  - subject
  - verb phrase 
  - object
 
   eg. Sachin Tendulkar was born in Mumbai.

   You can provide multiple sentences in the file but make sure each sentence is written on different line 
   and ends with a period (.)
- Run
  ``` hadoop jar PhraseBase.jar PhraseBase input.txt ```
