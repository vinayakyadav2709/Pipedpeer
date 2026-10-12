# Word frequency over a pile of documents, one document per process.
# The documents are made up (random words) so the script needs no input files.
# Writes the 50 most common words to counts.txt.
import random
import time
from collections import Counter
from multiprocessing import Pool

NUM_DOCS = 400
WORDS_PER_DOC = 1_000_000

VOCAB = ("the of and to in is was he for it with as his on be at by i this had "
         "not are but from or have an they which one you were her all she there "
         "would their we him been has when who will more no if out so said what "
         "up its about into than them can only other new some could time these "
         "two may then do first any my now such like our over man me even most "
         "made after also did many before must through back years where much "
         "your way well down should because each just those people mr how too "
         "little state good very make world still own see men work long get here "
         "between both life being under never day same another know while last").split()
PUNCT = ".,;:!?\"'()"


def make_document(doc_id):
    rng = random.Random(doc_id)
    weights = [1.0 / (rank + 1) for rank in range(len(VOCAB))]
    words = rng.choices(VOCAB, weights=weights, k=WORDS_PER_DOC)
    words = [w.capitalize() + "," if i % 9 == 0 else w for i, w in enumerate(words)]
    return " ".join(words)


def count_words(doc_id):
    text = make_document(doc_id)
    counts = {}
    for word in text.split():
        word = word.strip(PUNCT).lower()
        if word:
            counts[word] = counts.get(word, 0) + 1
    return counts


if __name__ == "__main__":
    t0 = time.time()
    print("counting words in", NUM_DOCS, "documents")

    with Pool() as p:
        per_doc = p.map(count_words, range(NUM_DOCS))

    total = Counter()
    for c in per_doc:
        total.update(c)

    with open("counts.txt", "w") as f:
        for word, n in total.most_common(50):
            f.write("%s %d\n" % (word, n))

    print("distinct words:", len(total), " total words:", sum(total.values()))
    print("top 5:", total.most_common(5))
    print("wrote counts.txt")
    print("TOTAL %.1fs" % (time.time() - t0))
