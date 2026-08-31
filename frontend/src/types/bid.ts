export interface Bid {
  id: string;
  source_id: string;
  external_id: string;
  bid_number: string;
  title: string;
  description: string;
  agency: string;
  agency_type: string;
  posted_date: string | null;
  due_date: string | null;
  estimated_value: number | null;
  location_state: string;
  location_county: string;
  location_city: string;
  naics_code: string;
  naics_description: string;
  status: string;
  bid_url: string;
  match_score: number;
  matched_keywords: string;
  fetched_at: string;
}

export interface BidFilters {
  counties: string[];
  sources: string[];
  agencyTypes: string[];
  minScore: number;
  search: string;
}

export const ESBD_SOURCE_ID = "texas_esbd";

export const DFW_COUNTIES = [
  "Dallas", "Tarrant", "Collin", "Denton", "Rockwall",
  "Kaufman", "Ellis", "Johnson", "Parker", "Wise", "Hunt", "Grayson",
];

export const SOURCE_LABELS: Record<string, string> = {
  texas_esbd:             "ESBD",
  bidsync:                "BidSync",
  fort_worth_bonfire:     "FW Bonfire",
  dallas_bonfire:         "Dallas Bonfire",
  dallas_isd_bonfire:     "Dallas ISD",
  richardson_isd_bonfire: "Richardson ISD",
  rockwall_isd_bonfire:   "Rockwall ISD",
  ionwave_arlington_isd:  "Arlington ISD",
  ionwave_mesquite_isd:   "Mesquite ISD",
  ionwave_tarrant_county: "Tarrant Co",
  ionwave_irving:         "Irving",
  ionwave_plano:          "Plano",
  ionwave_fwisd:          "FWISD",
  ionwave_nwisd:          "NWISD",
  bidnet:                 "BidNet",
  sam_gov:                "SAM.gov",
};

export const SOURCE_COLORS: Record<string, string> = {
  texas_esbd:             "bg-blue-500/20 text-blue-300",
  bidsync:                "bg-purple-500/20 text-purple-300",
  fort_worth_bonfire:     "bg-orange-500/20 text-orange-300",
  dallas_bonfire:         "bg-orange-500/20 text-orange-300",
  dallas_isd_bonfire:     "bg-orange-500/20 text-orange-300",
  richardson_isd_bonfire: "bg-orange-500/20 text-orange-300",
  rockwall_isd_bonfire:   "bg-orange-500/20 text-orange-300",
  ionwave_arlington_isd:  "bg-teal-500/20 text-teal-300",
  ionwave_mesquite_isd:   "bg-teal-500/20 text-teal-300",
  ionwave_tarrant_county: "bg-teal-500/20 text-teal-300",
  ionwave_irving:         "bg-teal-500/20 text-teal-300",
  ionwave_plano:          "bg-teal-500/20 text-teal-300",
  ionwave_fwisd:          "bg-teal-500/20 text-teal-300",
  ionwave_nwisd:          "bg-teal-500/20 text-teal-300",
  sam_gov:                "bg-red-500/20 text-red-300",
  bidnet:                 "bg-gray-500/20 text-gray-300",
};
