package main

// station is one weather station: a name and its mean annual
// temperature in Celsius, used as the center of a per-station Gaussian
// distribution when generating synthetic rows.
type station struct {
	name string
	mean float64
}

// stations is a 100-city subset in the same spirit as the 1BRC
// challenge's ~413-station list: real city names with roughly plausible
// mean annual temperatures, used to generate a "station;temperature"
// dataset without depending on the original (much larger) file.
var stations = []station{
	{"Abidjan", 26.0}, {"Abu Dhabi", 28.3}, {"Algiers", 18.2}, {"Amsterdam", 10.7},
	{"Ankara", 12.0}, {"Athens", 18.9}, {"Auckland", 15.2}, {"Baghdad", 22.8},
	{"Bangkok", 28.6}, {"Barcelona", 18.2}, {"Beijing", 12.5}, {"Beirut", 20.9},
	{"Berlin", 10.3}, {"Bogota", 13.8}, {"Boston", 10.9}, {"Brisbane", 21.4},
	{"Brussels", 10.5}, {"Bucharest", 11.0}, {"Budapest", 11.3}, {"Buenos Aires", 17.9},
	{"Cairo", 21.9}, {"Calgary", 4.4}, {"Cape Town", 16.7}, {"Caracas", 25.8},
	{"Casablanca", 17.6}, {"Chicago", 9.9}, {"Copenhagen", 9.1}, {"Dakar", 24.4},
	{"Dallas", 19.0}, {"Delhi", 25.2}, {"Denver", 10.4}, {"Dhaka", 25.9},
	{"Dubai", 27.0}, {"Dublin", 9.8}, {"Edinburgh", 9.3}, {"Frankfurt", 10.6},
	{"Geneva", 10.2}, {"Glasgow", 8.8}, {"Hanoi", 23.6}, {"Havana", 25.2},
	{"Helsinki", 5.9}, {"Ho Chi Minh City", 27.4}, {"Hong Kong", 23.3}, {"Honolulu", 25.4},
	{"Houston", 20.8}, {"Istanbul", 14.0}, {"Jakarta", 26.7}, {"Johannesburg", 15.5},
	{"Kabul", 12.1}, {"Karachi", 26.0}, {"Kathmandu", 18.3}, {"Kinshasa", 25.3},
	{"Kuala Lumpur", 27.3}, {"Lagos", 26.7}, {"Lima", 18.9}, {"Lisbon", 17.4},
	{"London", 11.3}, {"Los Angeles", 18.6}, {"Madrid", 15.0}, {"Manila", 27.4},
	{"Melbourne", 15.1}, {"Mexico City", 17.5}, {"Miami", 24.9}, {"Milan", 13.0},
	{"Minneapolis", 7.7}, {"Montreal", 6.8}, {"Moscow", 5.8}, {"Mumbai", 27.2},
	{"Nairobi", 17.9}, {"New Delhi", 25.2}, {"New York City", 12.9}, {"Osaka", 16.9},
	{"Oslo", 5.7}, {"Ottawa", 6.6}, {"Paris", 12.3}, {"Perth", 18.7},
	{"Philadelphia", 12.9}, {"Phoenix", 23.6}, {"Prague", 8.4}, {"Quebec City", 4.4},
	{"Reykjavik", 4.3}, {"Riga", 6.2}, {"Rio de Janeiro", 23.8}, {"Riyadh", 26.0},
	{"Rome", 15.2}, {"San Francisco", 14.6}, {"Santiago", 14.4}, {"Sao Paulo", 19.5},
	{"Seattle", 11.3}, {"Seoul", 12.5}, {"Shanghai", 16.7}, {"Singapore", 27.0},
	{"Stockholm", 6.6}, {"Sydney", 17.7}, {"Taipei", 23.0}, {"Tehran", 17.0},
	{"Tokyo", 15.4}, {"Toronto", 9.4}, {"Vancouver", 10.4}, {"Vienna", 10.4},
	{"Warsaw", 8.5}, {"Washington DC", 14.6}, {"Wellington", 12.9}, {"Zurich", 9.3},
}
