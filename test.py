class IdentifiableEntity(object):
    def __init__(self, id):
        self.id = id

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result


class Publication(IdentifiableEntity):
    def __init__(self, id, publication_year, title, publicationVenue):

        self.publication_year = publication_year
        self.title = title
        self.PublicationVenue = publicationVenue
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.publication_year, self.title, self.PublicationVenue])

    def getPublicationYear(self):
        if self.publication_year:
            return self.publication_year

    def getTitle(self):
        return self.title

    def getPublicationVenue(self):
        return self.getPublicationVenue


class Person(IdentifiableEntity):
    def __init__(self, id, givenName, familyName):

        self.givenName = givenName
        self.familyName = familyName
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.givenName, self.familyName])

    def getGivenName(self):
        return self.givenName

    def getFamilyName(self):
        return self.familyName


class Venue(IdentifiableEntity):  # issn_isbn is id
    def __init__(self, id, publication_venue, publisher):  # issn_isbn self.issn_isbn = issn_isbn
        self.publisher = publisher
        self.publication_venue = publication_venue
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.publication_venue, self.publisher])

    def getPublicationVenue(self):
        return self.publication_venue

    def getPublisher(self):
        return self.publisher


class Organization(IdentifiableEntity):
    def __init__(self, id, name):
        self.name = name
        super().__init__(id)

    def __str__(self):
        return str([self.id, self.name])

    def __str__(self):
        return str([self.id, self.name])

    def getName(self):
        return self.name


class JournalArticle(Publication):
    def __init__(self, id, publication_year, title, publication_venue, issue, volume):
        self.publication_venue = publication_venue
        self.issue = issue
        self.volume = volume
        super().__init__(id, publication_year, title, publication_venue)

    def __str__(self):
        return str([self.id, self.publication_year, self.title, self.publication_venue, self.issue, self.volume])

    def getIssue(self):
        if self.issue:
            return self.issue
        else:
            return None

    def getVolume(self):
        if self.volume:
            return self.volume
        else:
            return None


class BookChapter(Publication):
    def __init__(self, id, publication_year, title, publicationVenue, chapterNumber):
        self.chapterNumber = chapterNumber
        super().__init__(id, publication_year, title, publicationVenue)

    def getChapterNumber(self):
        return self.chapterNumber


class ProceedingsPaper(Publication):
    def __init__(self, id, publication_year, title, publicationVenue):
        super().__init__(id, publication_year, title, publicationVenue)


class Journal(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)


class Book(Venue):
    def __init__(self, id, title, publisher):
        super().__init__(id, title, publisher)


class Proceedings(Venue):
    def __init__(self, id, publication_venue, publisher, event):
        self.event = event
        super().__init__(id, publication_venue, publisher)

    def __str__(self):
        return str([self.id, self.publication_venue, self.publisher, self.event])

    def getEvent(self):
        return self.event

id = IdentifiableEntity(["id1", "id2"])
print("The ids of id are:", id.getIds())

publication1 = Publication(["id1", "id2"], 2020, "Title", "venue") #dobbiamo capire cosa mettiamo come venue 
print("The ids of id are:" , publication1.getIds())
print("The year of publication of publication1 is:", publication1.getPublicationYear())
print("The title of publication1 is:", publication1.getTitle())

person = Person(["0000-0003-0327-638X"], "Jan-Willem", "Boiten")
print("The id of person is:", person.getIds())
print("The given name of person is:", person.getGivenName())
print("The family name of person is:", person.getFamilyName())

organization = Organization(["crossref:98"], "Hindawi Limited")
print("The id of organization is:", organization.getIds())
print("The name of organization is:", organization.getName())

journalarticleexample = JournalArticle(["id3"], "2021", "Title", "venue", "2", "9" )
print("journalarticle's id is:", journalarticleexample.getIds())
print("journalarticle's publication year is:", journalarticleexample.getPublicationYear())
print("journalarticle's title is:", journalarticleexample.getTitle())
print("journalarticle's issue is:", journalarticleexample.getIssue())
print("journalarticle's venue is:", journalarticleexample.getPublicationVenue())
print("journalarticle's volume is:", journalarticleexample.getVolume())

bookchapterexample = BookChapter(["id4"], "2019", "Title", "Venue", "2")
print("The id of bookchapetexample is:", bookchapterexample.getIds())
print("The publication year of bookchapterexample is:", bookchapterexample.getPublicationYear())
print("The title of bookchapterexample is:", bookchapterexample.getTitle())
print("The chapter number of bookchapterexample is:", bookchapterexample.getChapterNumber())
print("The venue of bookchapterexample is:", bookchapterexample.getPublicationVenue())

proceedingpaperexample = ProceedingsPaper(["id5"], "2019", "Title", "Venue2")
print("The id of proceedingpaperexample is:", proceedingpaperexample.getIds())
print("The publication year of proceedingpaperexample is:", proceedingpaperexample.getPublicationYear())
print("The title of proceedingpaperexample is:", proceedingpaperexample.getTitle())
print("The venue of proceedingpaperexample is:", proceedingpaperexample.getPublicationVenue())

journalexample = Journal(["id6"], "Title",["crossref:2020"])
print("The id of journalexample is:", journalexample.getIds())
print("The title of journalexample is:", journalexample.getPublicationVenue())
print("The publisher of journalexample is:", journalexample.getPublisher())

bookexample = Book(["id7"], "Title",["crossref:2455"])
print("The id of bookexample is:", bookexample.getIds())
print("The title of bookexample is:", bookexample.getPublicationVenue())
print("The publisher of bookexample is:", bookexample.getPublisher())

proceedingexample = Proceedings(["id8"], "Title",["crossref:2435"])
print("The id of proceedingexample is:", proceedingexample.getIds())
print("The title of proceedingexample is:", proceedingexample.getPublicationVenue())
print("The publisher of proceedingexample is:", proceedingexample.getPublisher())








