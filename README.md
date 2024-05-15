> [!NOTE]  
> This is not yet a library, but rather a script that you can adjust to your own requirements.

# Nexis Clicker

> My PhD supervisor once told me that everyone doing newspaper analysis starts by writing code to read in files from the 'LexisNexis' newspaper archive. <br>
> – Johannes Gruber, author of [LexisNexisTools](https://github.com/JBGruber/LexisNexisTools)

_Nexis Uni_ is essentially the only source for newspaper articles and press releases[^1]. Since they do not have an API[^2], one needs to click through their database manually and download the articles bit by bit. This script automates the process to some extent by doing the clickwork for you. Just as for manual downloading, there are limits on the number of articles per download, and on the number of downloads per day. So it will still be a very slow and limited process – but at least you don't need to click yourself any more.

[^1]: For online newspaper articles, there is also the open portal [Media Cloud](https://mediacloud.org). For print newspaper articles in German-speaking countries, there is the [German Reference Corpus](https://www.ids-mannheim.de/digspra/kl/projekte/korpora/), but I am not aware of similar databases for other countries. I have searched extensively, and found only Nexis Uni and the other (Lexis)Nexis products.

[^2]: There is a "URL API", but that has nothing to do with a REST API.
