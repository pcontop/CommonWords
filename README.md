# CommonWords
A small scala + spark project. 

Done as an assignment to my Big Data post graduation course. It finds the 1500 most used common words between two html books:

1. It reads all the book a files from a parametrized base directory. 
2. Then it removes the html formatting from the files - using jsoup.
3. Then, it does a flatMap of all the words.
4. Finally, it creates a map with (word, count).
5. The steps 1-4 are repeated for file2. 
6. The words are merged between the map from file1 and file2. The counts from each book are added.
7. The 1500 words with the highest total count are saved to a "wordsInBothBooks.txt" ordered file. 
