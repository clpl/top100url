#include <iostream>
#include <queue>
#include <vector>
#include <fstream>
#include <cstdio>
#include <thread>
#include <map>
#include <algorithm>
#include <functional>

//Data structure to store url-frequency pairs
struct url_count
{
    std::string first;
    int second;

    bool operator < (const url_count &b) const
    {
        return second < b.second;
    }
};

long long g_file_size=0;
//Divide the file into blocks of different offsets, then threads handle the blocks
std::queue<long long> offset_que;
const long long NUM_THREADS = 4;
//the max length of url
const long long MAX_URL_SIZE = 65536;
//the block size = 1GB/NUM_THREADS*0.15(Experimental parameters). Limit memory usage less than 1G.
const long long MAX_BLOCK_SIZE = 1024*1024*1024/NUM_THREADS*0.15;
//hash slot, should be prime number.
const int SLOT_NUMBER = 307;

//hash function of string
std::hash<std::string> hs;

//read a url from FILE* point
//if read fail, return void string
std::string f_read_a_url(FILE *fin)
{
    if(feof(fin)) return std::string();
    char ch[MAX_URL_SIZE];
    int retcode = fscanf(fin,"%s",ch);
    if(retcode!=1) return std::string();
    std::string ans = std::string(ch);
    return ans;
}

//Divide the file into blocks of different offsets
void init_offsets_of_mappers(std::string input_file_path)
{
    FILE *fin = fopen(input_file_path.c_str(), "rb");
    if (fin == NULL)
    {
        printf("ERROR: File not found!\n");
    }

    //get file size
    fseek(fin, 0, SEEK_END);
    g_file_size = ftell(fin);

    //Divide files evenly to get divided offsets
    std::vector<long long> file_offsets;
    for(int i=0; i*MAX_BLOCK_SIZE<g_file_size; i++)
    {
        file_offsets.push_back(i*MAX_BLOCK_SIZE);
    }

    //Make sure offsets is in the '\n', not in the middle of urls
    for(int i=0; i < file_offsets.size(); i++)
    {
        fseek(fin, file_offsets[i], SEEK_SET);
        f_read_a_url(fin);
        file_offsets[i] = ftell(fin);
    }
    //Avoiding block sizes that are too small, causing offsets to repeat
    file_offsets.erase(unique(file_offsets.begin(), file_offsets.end()), file_offsets.end());
    file_offsets[0] = 0;
    for(int i=0; i < file_offsets.size(); i++)
    {
        offset_que.push(file_offsets[i]);
    }
}

std::vector<FILE *> init_hash_file(std::string directory_temp)
{
    std::vector<FILE *> vec;
    char output_file_name[200];
    for(int file_id=0; file_id<SLOT_NUMBER; file_id++)
    {
        sprintf(output_file_name, "%s/temp_%d.txt", directory_temp.c_str(), file_id);
        FILE* fout = fopen(output_file_name,"w");
        vec.push_back(fout);
    }
    return vec;
}

void close_hash_file(std::vector<FILE *> vec)
{
    for(auto c:vec)
    {
        fclose(c);
    }
}

//cal topK url from the map<std::string, int>
std::priority_queue<url_count> values_count_topK(const std::map<std::string, int> &mp, long long K=100)
{
    std::priority_queue<url_count> que;
    int counter = 0;
    for(auto c=mp.begin(); c!=mp.end(); c++)
    {
        if(counter++<K)
        {
            que.push({c->first,c->second});
        }
        else
        {
            url_count temp = que.top();
            if(temp.second<c->second)
            {
                que.pop();
                que.push({c->first,c->second});
            }
        }
    }
    return que;
}

//Count the url frequency for a block and write result to the hash file
void mapper(int id, std::string input_file_path, std::vector<FILE *> hash_file_vec)
{
    printf("map begin, thread id:%d\n",id);
    FILE *fin = fopen(input_file_path.c_str(), "rb");
    while(!offset_que.empty())
    {
        //get the begin offset and end offset from offset_que
        long long offset = offset_que.front();
        offset_que.pop();
        long long offsetEnd;

        //Make sure the last one doesn't cross the border of file's tail
        if(!offset_que.empty())
            offsetEnd = offset_que.front();
        else
            offsetEnd = g_file_size;

        //Processing the block
        fseek(fin, offset, SEEK_SET);
        std::map<std::string, int> mp;
        while(true)
        {
            long long nowpos = ftell(fin);
            if(feof(fin)||nowpos>=offsetEnd) break;
            std::string url = f_read_a_url(fin);
            if(url.empty()) break;
            if(mp.find(url)!=mp.end())
            {
                mp[url]++;
            }
            else
            {
                mp[url] = 1;
            }
        }

        for(auto c=mp.begin(); c!=mp.end(); c++)
        {
            //write url-fre result to different file according the hash value of url
            int hash_value = hs(c->first)%hash_file_vec.size();
            FILE * fout = hash_file_vec[hash_value];
            fprintf(fout,"%s %d\n",c->first.c_str(),c->second);
        }

    }

}

//start threads and join them
void map_entire_file(std::string input_file_path, std::vector<FILE *> hash_file_vec)
{
    std::thread* tarr[64];
    for(int i=0; i < NUM_THREADS; i++)
    {
        tarr[i] = new std::thread(mapper, i, input_file_path, hash_file_vec);
    }
    for(int i=0; i<NUM_THREADS; i++)
    {
        tarr[i]->join();
    }
}

//read hash files and reduce the url-fre pair. Then print result of top 100 url.
void reduce(std::string directory_temp)
{
    printf("reduce begin\n");
    char output_file_name[200];
    std::map<std::string, int> mp;
    std::priority_queue<url_count> que;
    //read hash files
    for(int fid=0; fid<SLOT_NUMBER; fid++)
    {
        mp.clear();

        //open file
        sprintf(output_file_name, "%s/temp_%d.txt", directory_temp.c_str(), fid);
        FILE* fin = fopen(output_file_name,"r");

        while(true)
        {
            if(feof(fin)) break;
            std::string url = f_read_a_url(fin);
            if(url.empty()) break;
            int fre=0;
            fscanf(fin, "%d", &fre);
            if(mp.find(url)!=mp.end())
            {
                mp[url] += fre;
            }
            else
            {
                mp[url] = fre;
            }
        }
        fclose(fin);

        //Add urls with a frequency greater than the heap's root node to the heap.
        for(auto c=mp.begin(); c!=mp.end(); c++)
        {
            if(que.size()<100)
            {
                que.push({c->first,c->second});
            }
            else
            {
                url_count temp = que.top();
                if(temp.second<c->second)
                {
                    que.pop();
                    que.push({c->first,c->second});
                }
            }
        }

    }

    //print result
    while(!que.empty())
    {
        url_count temp = que.top();
        que.pop();
        printf("%s %d\n",temp.first.c_str(),temp.second);
    }
}



int main()
{
    //input file
    std::string input_file_path = "test.txt";
    //hash file directory
    std::string directory_temp = "hash_file";

    init_offsets_of_mappers(input_file_path);

    std::vector<FILE *> hash_file_vec = init_hash_file(directory_temp);

    map_entire_file(input_file_path, hash_file_vec);

    close_hash_file(hash_file_vec);

    reduce(directory_temp);

    return 0;
}
