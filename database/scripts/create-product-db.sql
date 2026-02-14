--
-- PostgreSQL database dump
--

-- Dumped from database version 14.20 (Ubuntu 14.20-0ubuntu0.22.04.1)
-- Dumped by pg_dump version 16.0

-- Started on 2026-02-14 21:53:25 CET

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
-- TOC entry 4 (class 2615 OID 2200)
-- Name: public; Type: SCHEMA; Schema: -; Owner: postgres
--

CREATE SCHEMA public;


ALTER SCHEMA public OWNER TO postgres;

--
-- TOC entry 3338 (class 0 OID 0)
-- Dependencies: 4
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: postgres
--

COMMENT ON SCHEMA public IS 'standard public schema';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- TOC entry 212 (class 1259 OID 16399)
-- Name: product_stock; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.product_stock (
    id_stock integer NOT NULL,
    id_catalog_product integer NOT NULL,
    size_label character varying(10) NOT NULL,
    quantity_available integer DEFAULT 0
);


ALTER TABLE public.product_stock OWNER TO postgres;

--
-- TOC entry 211 (class 1259 OID 16398)
-- Name: product_stock_id_stock_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.product_stock_id_stock_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.product_stock_id_stock_seq OWNER TO postgres;

--
-- TOC entry 3340 (class 0 OID 0)
-- Dependencies: 211
-- Name: product_stock_id_stock_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.product_stock_id_stock_seq OWNED BY public.product_stock.id_stock;


--
-- TOC entry 210 (class 1259 OID 16389)
-- Name: ref_product_catalog; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.ref_product_catalog (
    id_catalog_product integer NOT NULL,
    reference character varying(50) NOT NULL,
    name character varying(255),
    brand character varying(100),
    color character varying(50),
    season character varying(50),
    sizes character varying(100),
    gender_segment character varying(50),
    main_category character varying(50),
    sub_category character varying(50),
    article_type character varying(50),
    score_ec integer DEFAULT 0,
    price double precision
);


ALTER TABLE public.ref_product_catalog OWNER TO postgres;

--
-- TOC entry 209 (class 1259 OID 16388)
-- Name: ref_product_catalog_id_catalog_product_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.ref_product_catalog_id_catalog_product_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.ref_product_catalog_id_catalog_product_seq OWNER TO postgres;

--
-- TOC entry 3341 (class 0 OID 0)
-- Dependencies: 209
-- Name: ref_product_catalog_id_catalog_product_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.ref_product_catalog_id_catalog_product_seq OWNED BY public.ref_product_catalog.id_catalog_product;


--
-- TOC entry 214 (class 1259 OID 16537)
-- Name: warehouses; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.warehouses (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    city character varying(100),
    street character varying(255),
    zip_code character varying(20),
    country character varying(100) DEFAULT 'France'::character varying,
    gps_lat numeric(9,6),
    gps_long numeric(9,6)
);


ALTER TABLE public.warehouses OWNER TO postgres;

--
-- TOC entry 213 (class 1259 OID 16536)
-- Name: warehouses_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.warehouses_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE public.warehouses_id_seq OWNER TO postgres;

--
-- TOC entry 3342 (class 0 OID 0)
-- Dependencies: 213
-- Name: warehouses_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.warehouses_id_seq OWNED BY public.warehouses.id;


--
-- TOC entry 3183 (class 2604 OID 16402)
-- Name: product_stock id_stock; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_stock ALTER COLUMN id_stock SET DEFAULT nextval('public.product_stock_id_stock_seq'::regclass);


--
-- TOC entry 3181 (class 2604 OID 16392)
-- Name: ref_product_catalog id_catalog_product; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ref_product_catalog ALTER COLUMN id_catalog_product SET DEFAULT nextval('public.ref_product_catalog_id_catalog_product_seq'::regclass);


--
-- TOC entry 3185 (class 2604 OID 16540)
-- Name: warehouses id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.warehouses ALTER COLUMN id SET DEFAULT nextval('public.warehouses_id_seq'::regclass);


--
-- TOC entry 3190 (class 2606 OID 16405)
-- Name: product_stock product_stock_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_stock
    ADD CONSTRAINT product_stock_pkey PRIMARY KEY (id_stock);


--
-- TOC entry 3188 (class 2606 OID 16397)
-- Name: ref_product_catalog ref_product_catalog_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.ref_product_catalog
    ADD CONSTRAINT ref_product_catalog_pkey PRIMARY KEY (id_catalog_product);


--
-- TOC entry 3192 (class 2606 OID 16543)
-- Name: warehouses warehouses_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.warehouses
    ADD CONSTRAINT warehouses_pkey PRIMARY KEY (id);


--
-- TOC entry 3193 (class 2606 OID 16406)
-- Name: product_stock fk_catalog_product; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.product_stock
    ADD CONSTRAINT fk_catalog_product FOREIGN KEY (id_catalog_product) REFERENCES public.ref_product_catalog(id_catalog_product) ON DELETE CASCADE;


--
-- TOC entry 3339 (class 0 OID 0)
-- Dependencies: 4
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: postgres
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO PUBLIC;


-- Completed on 2026-02-14 21:53:30 CET

--
-- PostgreSQL database dump complete
--

