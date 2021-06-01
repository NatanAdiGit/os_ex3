//
// Created by natan on 6/1/2021.
//

#include <pthread.h>
#include "MapReduceFramework.h"

IntermediateVec libInterVec;
OutputVec libOutputVec;
int numOfThreads;
pthread_t *threads;


typedef struct ClientHolder {
    const MapReduceClient* client;
    const InputPair* pair;
}ClientHolder;


void * ClientMap(void* args) {
    ClientHolder holder = *(ClientHolder*)args;
    holder.client->map(holder.pair->first, holder.pair->second, &libInterVec)
}

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec, OutputVec& outputVec,
                            int multiThreadLevel) {
    numOfThreads = multiThreadLevel;

    threads = new pthread_t[multiThreadLevel];

    for (int i = 0; i < numOfThreads && i < inputVec.size() ; ++i) {
        ClientHolder clientHolder;
        clientHolder.client = &client;
        clientHolder.pair = &inputVec[i];
        pthread_create(threads + i, NULL, ClientMap, (void *) &clientHolder);
    }



    libOutputVec = outputVec;
}

void waitForJob(JobHandle job) {

}

void getJobState(JobHandle job, JobState* state) {

}

void closeJobHandle(JobHandle job) {

}

void emit2 (K2* key, V2* value, void* context) {

}

void emit3 (K3* key, V3* value, void* context) {

}
