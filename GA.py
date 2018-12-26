# -*- coding: utf-8 -*-
"""
Created on Sun Sep 13 15:06:22 2015

@author: t-tzhao
"""
import random
import time


class GeneticAlgorithm(object):
    def __init__(self, genetics):
        self.genetics = genetics
        pass

    def run(self):
        population = self.genetics.initial()
        while True:
            fits_pops = [(self.genetics.fitness(sorted(ch)),  sorted(ch)) for ch in population]
            if self.genetics.check_stop(fits_pops): break
            population = self.next(fits_pops)
            pass

        return population

    def next(self, fits):
        parents_generator = self.genetics.parents(fits)
        # time.sleep(2)
        # print self.genetics.elitism(fits)
        size = len(fits)-1
        nexts = []
        while len(nexts) < size:
            parents = next(parents_generator)
            cross = random.random() < self.genetics.probability_crossover()
            children = self.genetics.crossover(parents) if cross else parents

            for ch in children:
                mutate = random.random() < self.genetics.probability_mutation()
                nexts.append(self.genetics.mutation(ch) if mutate else ch)
                pass
            pass
        
        nexts = nexts[0:size]
        nexts.append(self.genetics.elitism(fits))
        # time.sleep(2)
        
        return nexts
        # return nexts[0:size]
    pass

class GeneticFunctions(object):
     def probability_crossover(self):
         r"""returns rate of occur crossover(0.0-1.0)"""
         return 1.0

     def probability_mutation(self):
         r"""returns rate of occur mutation(0.0-1.0)"""
         return 0.0

     def initial(self):
         r"""returns list of initial population
         """
         return []

     def fitness(self, chromosome):
         r"""returns domain fitness value of chromosome
         """
         return len(chromosome)

     def check_stop(self, fits_populations):
         r"""stop run if returns True
         - fits_populations: list of (fitness_value, chromosome)
         """
         return False

     def parents(self, fits_populations):
         r"""generator of selected parents
         """
         gen = iter(sorted(fits_populations))
         while True:
             f1, ch1 = next(gen)
             f2, ch2 = next(gen)
             yield (ch1, ch2)
             pass
         return

     def crossover(self, parents):
         r"""breed children
         """
         return parents

     def mutation(self, chromosome):
         r"""mutate chromosome
         """
         return chromosome

     def elitism(self,fits_populations):
         return list(fits_populations)[0]

     pass
 



