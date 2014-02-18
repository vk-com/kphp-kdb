#/bin/sh
gperf -CGD -c -N get_type -Z KeywordsSet -K keyword -L C++ -t keywords.gperf > keywords_set.hpp
