#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>   // fabs

#define MAX_IP_LEN 16
#define MAX_LINE_LEN 256
#define MAX_CONNECTIONS 10000
#define INITIAL_PACKET_CAPACITY 100000

typedef struct {
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
} Connection;

typedef struct {
    long long arrival_time;
    char src_ip[MAX_IP_LEN];
    int src_port;
    char dst_ip[MAX_IP_LEN];
    int dst_port;
    int length;
    double weight;
    int has_weight;
    char original_line[MAX_LINE_LEN];

    // WFQ specific fields
    double virtual_start_time;
    double virtual_finish_time;
    int connection_id;
    int appearance_order;
    char is_on_bus;
} Packet;

typedef struct {
    Connection conn;
    double weight;
    double virtual_finish_time;
    int appearance_order;
    int active;
} ConnectionInfo;

typedef struct {
    Packet *packets;
    int count;
    int capacity;
} PacketQueue;

// Global state
ConnectionInfo connections[MAX_CONNECTIONS];
int num_connections = 0;
double virtual_time = 0.0;
long long real_time = 0; // Represents when the server becomes free next
PacketQueue pending_packets = {NULL, 0, 0};
PacketQueue ready_queue = {NULL, 0, 0};
long long last_virtual_change = 0;
long long current_time = 0;
char is_packet_on_bus = 0;
char packet_on_bus_idx = 0;

// Function prototypes
int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order);
void parse_packet(const char* line, Packet* packet, int appearance_order);
void add_packet_to_queue(PacketQueue* queue, const Packet* packet);
void remove_packet_from_queue(PacketQueue* queue, int index);
int compare_packets_by_arrival_time(const void* a, const void* b);
void schedule_next_packet(int current_time);
void init_packet_queue(PacketQueue* queue);
void cleanup();
char* my_strdup(const char* s);
double sum_Active_weights(int num_active_ids, int active_conn_ids[], double current_weight_sum);

// Helper function to duplicate string (portable strdup)
double sum_Active_weights(int num_active_ids, int active_conn_ids[], double current_weight_sum) {
    for (int i = 0; i < ready_queue.count; i++) {
        int conn_id = ready_queue.packets[i].connection_id;
        int found = 0;
        for(int k=0; k < num_active_ids; ++k) {
            if(active_conn_ids[k] == conn_id) {
                found = 1;
                break;
            }
        }
        if(!found){
            current_weight_sum += connections[conn_id].weight;
            active_conn_ids[num_active_ids++] = conn_id;
        }
    }
    return current_weight_sum;
}
char* my_strdup(const char* s) {
    size_t len = strlen(s) + 1;
    char* copy = malloc(len);
    if (copy) {
        memcpy(copy, s, len);
    }
    return copy;
}

// Comparison function for qsort
int compare_packets_by_arrival_time(const void* a, const void* b) {
    const Packet* pa = (const Packet*)a;
    const Packet* pb = (const Packet*)b;
    if (pa->arrival_time < pb->arrival_time) return -1;
    if (pa->arrival_time > pb->arrival_time) return 1;
    return pa->appearance_order - pb->appearance_order;
}

int main() {
    char line[MAX_LINE_LEN];
    int appearance_order = 0;
    init_packet_queue(&pending_packets);
    init_packet_queue(&ready_queue);

    while (fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\n")] = 0;
        if (strlen(line) == 0) continue;
        Packet packet;
        parse_packet(line, &packet, appearance_order++);
        add_packet_to_queue(&pending_packets, &packet);
    }

    qsort(pending_packets.packets, pending_packets.count, sizeof(Packet), compare_packets_by_arrival_time);


    while (pending_packets.count > 0 || ready_queue.count > 0) {
        long long next_arrival_event_time = (pending_packets.count > 0) ? pending_packets.packets[0].arrival_time : LLONG_MAX;

        if (ready_queue.count == 0 && next_arrival_event_time == LLONG_MAX) {
            break;
        }
        if (ready_queue.count == 0) {
            current_time = next_arrival_event_time;
        } else {
            current_time = (next_arrival_event_time < real_time) ? next_arrival_event_time : real_time;
        }
        if (current_time >= real_time && is_packet_on_bus) {
            remove_packet_from_queue(&ready_queue, packet_on_bus_idx);
            is_packet_on_bus = 0;
        }


        if (current_time == LLONG_MAX) break;

        // **Potential Refinement for Idle Period Virtual Time Update**
        // If the system was idle (ready_queue empty before this current_time step)
        // and current_time (driven by an arrival) is ahead of virtual_time,
        // then advance virtual_time to catch up to current_time.
        // This should happen BEFORE processing arrivals for this current_time.
        int was_idle_before_processing_arrivals = (ready_queue.count == 0);
        if (ready_queue.count == 0 && virtual_time < current_time) { //TODO check this!!
            // System was idle: advance virtual time to match arrival
            virtual_time = current_time;
            last_virtual_change = current_time;
        }
        // else {
        //     int active_conn_ids[MAX_CONNECTIONS];
        //     int weights = sum_Active_weights(0, active_conn_ids, 0 );
        //     if (weights != 0) {
        //         virtual_time += (double)(current_time - real_time) / weights;
        //     }
        // }

        // Process all packets that have arrived by this current_time
        while (pending_packets.count > 0 && pending_packets.packets[0].arrival_time <= current_time) {
            Packet packet = pending_packets.packets[0];
            remove_packet_from_queue(&pending_packets, 0);

            int conn_id = packet.connection_id;
            double last_conn_vft = connections[conn_id].virtual_finish_time;

            double virtual_start = (virtual_time > last_conn_vft) ? virtual_time : last_conn_vft;

            // if (packet.arrival_time == 312232  || packet.arrival_time ==  312077) {
            //     // printf("DEBUG: %s virtual start: %f lastconfft %f virtual time %f \n", packet.original_line, virtual_start, last_conn_vft, virtual_time);
            // }

            // printf("DEBUG: last_conn_vft %lf, virtual_time %lf, chose %lf \n", last_conn_vft, virtual_time, virtual_start);

            packet.virtual_start_time = virtual_start;
            packet.virtual_finish_time = virtual_start + (double)packet.length / connections[conn_id].weight;

            connections[conn_id].virtual_finish_time = packet.virtual_finish_time;
            add_packet_to_queue(&ready_queue, &packet);
        }

        if (ready_queue.count > 0 && real_time <= current_time && is_packet_on_bus == 0) {
            schedule_next_packet(current_time);
        }
        else {
            if (ready_queue.count == 0 && virtual_time < current_time) {
                virtual_time = current_time;
                last_virtual_change = current_time;
            } else {
                int active_conn_ids[MAX_CONNECTIONS];
                int weight_sum = sum_Active_weights(0, active_conn_ids, 0);
                if (weight_sum > 0) {
                    virtual_time += (double)(current_time - last_virtual_change) / weight_sum;
                    last_virtual_change = current_time;
                }
            }
        }
    }

    cleanup();
    return 0;
}

void init_packet_queue(PacketQueue* queue) {
    queue->capacity = INITIAL_PACKET_CAPACITY;
    queue->packets = malloc(queue->capacity * sizeof(Packet));
    queue->count = 0;
}

int find_or_create_connection(const char* src_ip, int src_port, const char* dst_ip, int dst_port, int appearance_order) {
    // Look for existing connection
    for (int i = 0; i < num_connections; i++) {
        if (strcmp(connections[i].conn.src_ip, src_ip) == 0 &&
            connections[i].conn.src_port == src_port &&
            strcmp(connections[i].conn.dst_ip, dst_ip) == 0 &&
            connections[i].conn.dst_port == dst_port) {
            return i;
        }
    }

    // Create new connection
    if (num_connections >= MAX_CONNECTIONS) {
        fprintf(stderr, "Too many connections\n");
        exit(1);
    }

    int id = num_connections++;
    strcpy(connections[id].conn.src_ip, src_ip);
    connections[id].conn.src_port = src_port;
    strcpy(connections[id].conn.dst_ip, dst_ip);
    connections[id].conn.dst_port = dst_port;
    connections[id].weight = 1; // Default weight - THIS MUST BE 1, NOT 0!
    connections[id].virtual_finish_time = 0.0;
    connections[id].appearance_order = appearance_order;
    connections[id].active = 0;

    return id;
}

void parse_packet(const char* line, Packet* packet, int appearance_order) {
    strcpy(packet->original_line, line);
    packet->appearance_order = appearance_order;
    packet->has_weight = 0;
    packet -> is_on_bus = 0;

    char* line_copy = my_strdup(line);
    char* token = strtok(line_copy, " ");
    int field = 0;

    while (token != NULL) {
        switch (field) {
            case 0: packet->arrival_time = atoll(token); break;
            case 1: strcpy(packet->src_ip, token); break;
            case 2: packet->src_port = atoi(token); break;
            case 3: strcpy(packet->dst_ip, token); break;
            case 4: packet->dst_port = atoi(token); break;
            case 5: packet->length = atoi(token); break;
            case 6:
                packet->weight = atof(token);
                packet->has_weight = 1;
                break;
        }
        field++;
        token = strtok(NULL, " ");
    }

    free(line_copy);

    // Find or create connection
    packet->connection_id = find_or_create_connection(packet->src_ip, packet->src_port,
                                                     packet->dst_ip, packet->dst_port,
                                                     appearance_order);

    // Update connection weight if specified
    if (packet->has_weight) {
        connections[packet->connection_id].weight = packet->weight;
    } else {
        packet->weight = connections[packet->connection_id].weight;
    }
}

void add_packet_to_queue(PacketQueue* queue, const Packet* packet) {
    if (queue->count >= queue->capacity) {
        queue->capacity *= 2;
        queue->packets = realloc(queue->packets, queue->capacity * sizeof(Packet));
    }
    queue->packets[queue->count++] = *packet;
}

void remove_packet_from_queue(PacketQueue* queue, int index) {
    for (int i = index; i < queue->count - 1; i++) {
        queue->packets[i] = queue->packets[i + 1];
    }
    queue->count--;
}

void schedule_next_packet(int current_time) {
    if (ready_queue.count == 0) return;

    const double EPS = 1e-9;   // tolerance for almost-equal VFTs

    int best_idx = 0;
    for (int i = 1; i < ready_queue.count; i++) {
        if(ready_queue.packets[i].is_on_bus == 0) {
            double diff = ready_queue.packets[i].virtual_finish_time -
                          ready_queue.packets[best_idx].virtual_finish_time;

            if (diff < -EPS ||                            /* clearly smaller VFT          */
                (fabs(diff) <= EPS &&                   /* virtually equal → tie-break  */
                 connections[ready_queue.packets[i].connection_id].appearance_order <
                 connections[ready_queue.packets[best_idx].connection_id].appearance_order)) {
                best_idx = i;
                 }
        }
    }
        ready_queue.packets[best_idx].is_on_bus = 1;
        Packet packet_to_send = ready_queue.packets[best_idx];
    //remove_packet_from_queue(&ready_queue, best_idx);
        is_packet_on_bus = 1;
        packet_on_bus_idx = best_idx;

    // Determine actual start time for this packet
    // real_time currently holds when the server *became free* from the *previous* transmission (or 0 if idle)
    long long actual_start_time = (real_time > packet_to_send.arrival_time) ? real_time : packet_to_send.arrival_time;

    // Original output format restored
    printf("%lld: %s\n", actual_start_time, packet_to_send.original_line);



    // Update virtual time
    // Calculate weight sum of connections that were in ready_queue *before* this packet was removed
    double current_weight_sum = 0;
    int active_conn_ids[MAX_CONNECTIONS]; // Stores unique connection IDs from ready_queue + packet_to_send
    int num_active_ids = 0;

    // Consider the packet_to_send as part of the active set for weight sum calculation
    //current_weight_sum += connections[packet_to_send.connection_id].weight; //TODO check uf ok to remove
    active_conn_ids[num_active_ids++] = packet_to_send.connection_id;

    // Add weights of other unique connections remaining in the ready_queue
    current_weight_sum = sum_Active_weights(num_active_ids, active_conn_ids, current_weight_sum);

    if (current_weight_sum > 0) {

        // printf("virtual time before %lf \n", virtual_time);
        virtual_time += (double)packet_to_send.length / current_weight_sum;
        last_virtual_change = current_time;
         // virtual_time += (double)(current_time - real_time) / current_weight_sum;
        // printf("virtual time after %lf \n", virtual_time);
     }
    // Update server's next free time
    real_time = actual_start_time + packet_to_send.length;
}

void cleanup() {
    if (pending_packets.packets) free(pending_packets.packets);
    if (ready_queue.packets) free(ready_queue.packets);
}