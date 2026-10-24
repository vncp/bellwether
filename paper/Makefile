# Build the Bellwether paper with tectonic.
#
#   make         compile main.tex to main.pdf
#   make clean   remove the PDF and any intermediate files
#
# tectonic fetches LaTeX packages on demand and runs BibTeX itself.

TECTONIC ?= tectonic

# data/*.csv: figure data written by `bellwether report --paper` (data/README.md);
# a panel draws its file when it exists and a placeholder otherwise.
SOURCES := main.tex references.bib $(wildcard sections/*.tex) $(wildcard figures/*.tex) \
           $(wildcard data/*.csv)

.PHONY: all clean

all: main.pdf

main.pdf: $(SOURCES)
	$(TECTONIC) main.tex

clean:
	rm -f main.pdf main.aux main.bbl main.blg main.log main.out main.xdv main.toc main.fls main.synctex.gz
	rm -rf build
