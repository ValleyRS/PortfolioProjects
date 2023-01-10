
/*
COVID-19 Data Exploration

Skills used: Joins, Windows Functions, Aggregate Functions, Creating Views, Temp Tables, CTE's, Converting Data Types
*/



--Datasets

select *
from PortfolioProject..CovidDeathsCleaned

select *
from PortfolioProject..CovidVaccinationsCleaned


--Data that we will be starting with

select location, date, total_cases, total_deaths
from PortfolioProject..CovidDeathsCleaned
where continent is not null
order by 3,4



--INFECTION AND MORTALITY DATA


--Total Cases vs Total Deaths

select location, date, total_cases, total_deaths, 
	(total_deaths/total_cases)*100 as DeathPercentage
from PortfolioProject..CovidDeathsCleaned
	where continent is not null
order by 1,2


--Shows Mortality Rate for Desired Country

select location, date, total_cases, total_deaths, 
	(total_deaths/total_cases)*100 as DeathPercentage
from PortfolioProject..CovidDeathsCleaned
	where location = 'United States'
order by 1,2


--Looking at Total Cases vs Population

select location, date, population, total_cases, 
	(total_cases/population)*100 as PercentPopulationInfected
from PortfolioProject..CovidDeathsCleaned
	where continent is not null
order by 1,2


--Total Cases vs Population for Specific Country

select location, date, population, total_cases, 
	(total_cases/population)*100 as PercentPopulationInfected
from PortfolioProject..CovidDeathsCleaned
	where location like '%Canada%'
order by 1,2


--Looking at Countries with Highest Infection Rate per Population

select location, population, 
	MAX(total_cases) as HighestInfectionCount, 
	MAX((total_cases/population))*100 as PercentPopulationInfected
from PortfolioProject..CovidDeathsCleaned
group by location, population
order by PercentPopulationInfected desc


--Showing Countries with Highest Mortality Rate

select location, 
	MAX(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject..CovidDeathsCleaned
	where continent is not null
group by location
order by TotalDeathCount desc


--GROUP BY CONTINENT

select location, MAX(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location not like '%income%' 
	and location not like '%world%'
	and location not like '%international%'
group by location
order by TotalDeathCount desc


--GROUP BY GLOBAL WAGE CLASSES

select location, MAX(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%income%'
group by location
order by TotalDeathCount desc



--GLOBAL NUMBERS


--GLOBAL Total Cases

select location, MAX(cast(total_cases as int)) as TotalCaseCount
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%world%'
group by location
order by TotalCaseCount desc


--GLOBAL Population Percent Infected

select location, MAX((total_cases/population))*100 as PercentPopulationInfected
from PortfolioProject..CovidDeathsCleaned
	where continent is null 
	and location like '%world%'
group by location
order by PercentPopulationInfected desc


--GLOBAL Total Deaths

select location, MAX(cast(total_deaths as int)) as TotalDeathCount
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%world%'
group by location
order by TotalDeathCount desc


--GLOBAL Percent of Population killed by Covid

select location, MAX((total_deaths/population)*100) as TotalDeathPercentage
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%world%'
group by location
order by TotalDeathPercentage desc


--GLOBAL Total Cases and Percent of Population Infected Combined

select location, MAX(cast(total_cases as int)) as TotalCaseCount,
	MAX((total_cases/population))*100 as PercentPopulationInfected
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%world%'
group by location
order by TotalCaseCount desc


--GLOBAL Total Deaths and Percent of Population killed Combined

select location, MAX(cast(total_deaths as int)) as TotalDeathCount,
	MAX((total_deaths/population)*100) as TotalDeathPercentage
from PortfolioProject..CovidDeathsCleaned
	where continent is null
	and location like '%world%'
group by location
order by TotalDeathCount desc



--TEST AND VACCINATION DATA


 --JOINED Death and Vaccination Data

select *
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date


--Total Population vs Total Vaccinations

select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(convert(bigint, vac.new_vaccinations)) 
	over (partition by dea.location order by dea.location, dea.date)
		as CumulativeVaccinations
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null
order by 2,3


-- Total Population vs Total Vaccinations for Specific Country

select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(convert(bigint, vac.new_vaccinations)) 
	over (partition by dea.location order by dea.location, dea.date)
		as CumulativeVaccinations
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null
		--Plug Desired Country in
	and dea.location like '%Kosovo%'
order by 2,3



--Total Population vs Total Vaccinations Cumulative PercentVaccinated

--Note: This includes a rolling count of initial shots and boosters,
--      that is why PercentVaccinated may exceed 100%.


--Using CTE to perform calculation on Partition By in previous query

with populationVSvaccination 
	(continent, location, date, population, new_vaccinations, CumulativeVaccinations)
as
(
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(convert(bigint, vac.new_vaccinations)) 
	over (partition by dea.location order by dea.location, dea.date)
		as CumulativeVaccinations
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null
)
Select *, 
(CumulativeVaccinations/Population)*100 as PercentVaccinated
From populationVSvaccination


--Now using a Temp Table to perform calculation on Partition By in previous query

drop table if exists #PercentPopVaccinated
create table #PercentPopVaccinated
(
Continent nvarchar(255),
Location nvarchar(255),
Date datetime,
Population numeric,
New_Vaccinations numeric,
CumulativeVaccinations numeric
)

insert into #PercentPopVaccinated
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(convert(bigint, vac.new_vaccinations))
	over (partition by dea.location order by dea.location, dea.date)
		as CumulativeVaccinations
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null
	and dea.location like 'United States'

select *,
(CumulativeVaccinations/Population)*100 as PercentVaccinated
from #PercentPopVaccinated




--Views to store data later for visualizations


--Rolling Count Vaccinations View

create view CumulativeVaccinations as
select dea.continent, dea.location, dea.date, dea.population, vac.new_vaccinations,
SUM(convert(bigint, vac.new_vaccinations))
	over (partition by dea.location order by dea.location, dea.date)
		as CumulativeVaccinations
from PortfolioProject..CovidDeathsCleaned dea
	join PortfolioProject..CovidVaccinationsCleaned vac
		on dea.location = vac.location
		and dea.date = vac.date
	where dea.continent is not null


