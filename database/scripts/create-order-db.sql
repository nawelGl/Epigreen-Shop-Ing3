--
-- PostgreSQL database dump
--

-- Dumped from database version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 16.0

-- Started on 2026-02-14 21:44:22 CET

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- TOC entry 3341 (class 0 OID 16498)
-- Dependencies: 209
-- Data for Name: orders; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.orders (id, customer_id, total_price, carbon_score_total, status, shipping_address_snapshot, created_at) FROM stdin;
\.


--
-- TOC entry 3343 (class 0 OID 16587)
-- Dependencies: 211
-- Data for Name: relay_points; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.relay_points (id, name, street, zip_code, city, country, gps_lat, gps_long) FROM stdin;
\.


--
-- TOC entry 3344 (class 0 OID 16596)
-- Dependencies: 212
-- Data for Name: deliveries; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.deliveries (id, order_id, tracking_number, mode, status, relay_point_id) FROM stdin;
\.


--
-- TOC entry 3342 (class 0 OID 16508)
-- Dependencies: 210
-- Data for Name: order_items; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.order_items (id, order_id, product_origin_id, product_name_snapshot, unit_price_snapshot, quantity) FROM stdin;
\.


-- Completed on 2026-02-14 21:44:28 CET

--
-- PostgreSQL database dump complete
--

