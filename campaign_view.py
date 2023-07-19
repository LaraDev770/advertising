from django import template
from django.template import loader
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, HttpResponseRedirect
from .models import Campaigns, CampaignsReports, Settings, Tags, CampaignPlacements
from django.http import JsonResponse
from django.db.models import Avg, Max, Min, Sum, Count, Value, F
from django.http import JsonResponse
from django.shortcuts import redirect
from django.http import HttpResponse
from django.template import Context, Template
from django.core.paginator import Paginator
from django.core.serializers import serialize
from advertising.helpers.FilterHelper import FilterHelper
from advertising.classes.BidChangeCampaign import BidChangeCampaign
from advertising.classes.StateChangeCampaign import StateChangeCampaign
from advertising.classes.StrategyChangeCampaign import StrategyChangeCampaign
from advertising.classes.PlacementBiddingChangeCampaign import PlacementBiddingChangeCampaign
from advertising.constants import *
import requests, datetime, json
import io, itertools, random
from django.core import serializers
from xlsxwriter.workbook import Workbook


@login_required(login_url="/login/")
def index(request, profile_id):
    filters = FilterHelper.getFilters(ENTITY_CAMPAIGNS, profile_id)
    choosed_columns = getChoosedColumns(profile_id, request)
    choosable_columns = tableReportingColumns(request)
    allTags = list(Tags.objects.filter(entity="campaigns", profile_id=profile_id).values('id', 'tag_name'))
    context = {'filters': filters, 'choosed_columns': choosed_columns, 'choosable_columns': choosable_columns,
               'allTags': allTags}
    html_template = loader.get_template('accounts/campaigns/index.html')
    return HttpResponse(html_template.render(context, request))


def tableEntityColumns(request):
    columns = {
        "name": "Campaign Name",
        "portfolio_name": "Portfolio Name",
        "tag_name": "Tag Name",
        "campaignType": "Campaign Type",
        "top_of_search": "Top of search %",
        "product_page": "Product Page %",
        "targetingType": "Targeting Type",
        "dailyBudget": "Daily Budget",
        "state": "State",
        "servingStatus": "Serving Status"}
    return columns


@login_required(login_url="/login/")
def tableReportingColumns(request):
    columns = {
        "name": "Campaign Name",
        "portfolio_name": "Portfolio Name",
        "tag_name": "Tag Name",
        "campaignType": "Campaign Type",
        "top_of_search": "Top of search %",
        "product_page": "Product Page %",
        "targetingType": "Targeting Type",
        "dailyBudget": "Daily Budget",
        "state": "State",
        "servingStatus": "Serving Status",
        "acos": "ACOS%",
        "impressions": "impressions",
        "clicks": "clicks",
        "cost": "cost",
        "ctr": "ctr%",
        "avgCpc": "avgCpc",
        "CR": "CR%",
        "purchases7d": "Purchases7d",
        "sales7d": "Sales7d",
        "unitsSoldClicks7d": "unitsSoldClicks7d",
        "attributedSalesSameSku7d": "SalesSameSku7d",
        "unitsSoldSameSku7d": "unitsSoldSameSku7d",
    }
    return columns


def prepareQuery(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters, is_export=0):
    page_no = int(page_no) - 1
    filter_query = FilterHelper.prepareFilterForQuery(filters)
    tag_filter = FilterHelper.prepareTagsFilterForQuery(filters)
    offset = str(0)
    if int(page_no) > 1:
        offset = (int(page_no) * int(per_page))

    limit = "limit {}  offset {}".format(per_page, offset)
    if (is_export):
        limit = ""

    query = "SELECT count(*) OVER() AS full_count,\
            et.id AS id,\
            et.profile_id AS profile_id,\
            p.internal_name AS profile_name,\
            pf.name AS portfolio_name,\
            NULL AS tag_name,\
            p.account_id AS account_id,\
            et.profileId AS profileId,\
            et.id AS campaignId,\
            et.campaignId AS amazonCampaignId,\
            et.campaignId AS entityId,\
            et.name AS name,\
            et.top_of_search AS top_of_search,\
            et.product_page AS product_page,\
            et.state AS state,\
            et.campaignType AS campaignType,\
            et.targetingType AS targetingType,\
            et.dailyBudget AS dailyBudget,\
            et.bidding AS bidding,\
            et.startDate AS startDate,\
            et.endDate AS endDate,\
            et.servingStatus AS servingStatus,\
            IFNULL(SUM(impressions), 0) AS impressions,\
            IFNULL(SUM(clicks),0) AS clicks,\
            IFNULL(SUM(cost),0) AS cost,\
            IF(SUM(impressions) = 0, NULL, round(COALESCE(CAST(SUM(clicks) AS DOUBLE)/SUM(impressions), 0)*100,2)) AS ctr,\
            IF(SUM(clicks) = 0, NULL, round(COALESCE(CAST(SUM(cost) AS DOUBLE)/SUM(clicks), 0),2)) AS avgCpc,\
            IF(SUM(clicks) = 0, NULL, round(COALESCE(CAST(SUM(purchases30d) AS DOUBLE)/SUM(clicks), 0)*100,2)) AS CR,\
            Round(IFNULL((SUM(cost)/SUM(sales7d))*100,0),2) AS acos,\
            IFNULL(SUM(purchases7d), 0) AS purchases7d,\
            IFNULL(SUM(sales7d), 0) AS sales7d,\
            IFNULL(SUM(unitsSoldClicks7d), 0) AS unitsSoldClicks7d,\
            IFNULL(SUM(attributedSalesSameSku7d), 0) AS attributedSalesSameSku7d,\
            IFNULL(SUM(unitsSoldSameSku7d), 0) AS unitsSoldSameSku7d\
            FROM `campaigns` AS `et`\
            LEFT JOIN portfolios AS pf ON `et`.`portfolioId` = `pf`.`portfolioId`\
            LEFT JOIN campaigns_reports AS rp ON `rp`.`campaignId` = `et`.`campaignId`\
            AND `report_date` >= '{}'\
            AND `report_date` <= '{}'\
            INNER JOIN `profiles` AS `p` ON `et`.`profile_id` = `p`.`id`\
            LEFT JOIN `tags` AS `t` ON FIND_IN_SET(et.campaignId, t.entity_ids)\
            WHERE 1=1\
            AND `et`.`profile_id` = {} {}\
            GROUP BY `campaignId` {} ORDER BY {} {} {}".format(
        datetime.datetime.strptime(startdate, "%Y%m%d").strftime("%Y-%m-%d"),
        datetime.datetime.strptime(enddate, "%Y%m%d").strftime("%Y-%m-%d"), profile_id, tag_filter, filter_query,
        order_by, order,
        limit)
    return query


def getData(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters):
    query = prepareQuery(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters)
    campaigns_reporting = Campaigns.objects.raw(query)
    return campaigns_reporting


def getAggregateRowQueryForTable(request, startdate, enddate, profile_id, page_no, per_page, order, order_by, filters):
    query = prepareQuery(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters)
    inner_query = query.split("limit")

    select_query = ""
    for col in tableReportingColumns(request):
        if col not in tableEntityColumns(request).keys():
            if col == "acos":
                select_query += "Round(IFNULL((SUM(cost)/SUM(sales7d))*100,0),2) AS acos,"
            elif col == "ctr":
                select_query += "IF(SUM(impressions) = 0, NULL, round(COALESCE(CAST(SUM(clicks) AS DOUBLE)/SUM(impressions), 0)*100,2)) AS ctr,"
            elif col == "CR":
                select_query += "IF( SUM(clicks) = 0, NULL, round((SUM(purchases7d)/SUM(clicks))*100,2)) AS CR,"
            elif col == "avgCpc":
                select_query += "IF(SUM(clicks) = 0, NULL, round(COALESCE(CAST(SUM(cost) AS DOUBLE)/SUM(clicks), 0),2)) AS avgCpc,"
            else:
                select_query += "ROUND(sum(" + col + "),2) AS " + col + ","

    select_query = select_query.rstrip(",")
    final_query = "SELECT *, " + select_query + " FROM (" + inner_query[0] + ") AS temp GROUP BY NULL"
    summary_result = Campaigns.objects.raw(final_query)
    return summary_result


def updateChoosedColumns(choosedColumns, profile_id):
    columns = Settings.objects.filter(profile_id=profile_id, type=ENTITY_CAMPAIGNS)
    if (columns):
        columns[0].columns = json.loads(choosedColumns)
        columns[0].save(update_fields=["columns"])
    else:
        columns = Settings.objects.create(name="Column", type=ENTITY_CAMPAIGNS, profile_id=profile_id,
                                          columns=json.loads(choosedColumns))

    return columns


def getChoosedColumns(profile_id, request):
    columns = Settings.objects.filter(profile_id=profile_id, type=ENTITY_CAMPAIGNS)
    if (columns):
        cols = columns[0].columns
        valid_json = cols.replace("\'", "\"")
        return json.loads(valid_json)
    else:
        return tableReportingColumns(request)


def ViewData(request, profile_id):
    data = {}
    viewdata = {}
    page_no = request.POST['page']
    per_page = request.POST['per_page']
    order = request.POST['order']
    order_by = request.POST['order_by']
    startdate = request.POST['startDate']
    enddate = request.POST['endDate']
    filters = request.POST['filters']
    choosed_columns = request.POST['choosed_columns']
    data["campaigns"] = getData(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters)
    updateChoosedColumns(choosed_columns, profile_id)
    data["summary_row"] = getAggregateRowQueryForTable(request, startdate, enddate, profile_id, page_no, per_page,
                                                       order,
                                                       order_by, filters)
    data["columns"] = getChoosedColumns(profile_id, request)
    data['entity_columns'] = tableEntityColumns(request)
    paginator = Paginator(data["campaigns"], per_page)
    if int(len(list(data["campaigns"]))) > 0:
        paginator.count = data["campaigns"][0].full_count
        data["full_count"] = data["campaigns"][0].full_count

        page_obj = paginator.get_page(page_no)
    else:
        data["full_count"] = 0
        paginator.count = 0
        page_obj = paginator.get_page(page_no)

    data["page_obj"] = page_obj
    data["order"] = order
    data["per_page"] = per_page
    data["order_by"] = order_by
    data['choosed_columns'] = getChoosedColumns(profile_id, ENTITY_CAMPAIGNS)
    html_template = loader.get_template('accounts/campaigns/table_data.html')
    html = html_template.render(data, request)
    viewdata["htmlData"] = html
    return JsonResponse(viewdata, safe=False)


def bidChange(request, profile_id):
    return BidChangeCampaign.bulkChange(request, profile_id)


def stateChange(request, profile_id):
    return StateChangeCampaign.stateChange(request, profile_id)


def strategyChange(request, profile_id):
    return StrategyChangeCampaign.strategyChange(request, profile_id)


def biddingPlacementChange(request, profile_id):
    return PlacementBiddingChangeCampaign.biddingPlacementChange(request, profile_id)


def add_campaigns_to_tag(request, profile_id):
    view_data = {}
    entityIds = request.POST.get('entityIds')  # Get the entityIds from AJAX request
    tag_id = request.POST.get('tag_id')  # Get the entityIds from AJAX request
    entityIds_list = entityIds.split(",")  # Split the entityIds into a list

    tag = Tags.objects.get(id=tag_id)
    existing_ids = tag.entity_ids.split(",") if tag.entity_ids else []  # Split existing entity_ids into a list

    # Remove duplicates from entityIds_list that already exist in existing_ids
    entityIds_list = [entity_id for entity_id in entityIds_list if entity_id not in existing_ids]

    updated_ids = existing_ids + entityIds_list  # Combine existing and new IDs

    tag.entity_ids = ",".join(updated_ids)  # Join the updated IDs list back into a comma-separated string
    tag.save()
    view_data["success"] = True
    return JsonResponse(view_data)


def create_new_tag_and_add_campaigns(request, profile_id):
    viewdata = {}
    viewdata["alreadyExists"] = 0
    tag_name = request.POST['tagName']
    current_user = request.user

    ifAlreadyExtsist = Tags.objects.filter(tag_name=tag_name, entity="campaigns")
    if (ifAlreadyExtsist):
        viewdata["alreadyExists"] = 1
        return JsonResponse(viewdata)

    tag = Tags.objects.create(tag_name=tag_name, entity="campaigns", created_by=current_user.id, profile_id=profile_id)
    allTags = Tags.objects.filter(entity="campaigns")
    serialized_tag = serialize('json', [tag])  # Serialize the tag object
    viewdata["tag"] = json.loads(serialized_tag)[0]  # Convert the serialized tag into JSON-serializable format
    viewdata["allTags"] = list(allTags.values())  # Serialize allTags queryset

    return JsonResponse(viewdata)


def delete_tag(request, profile_id):
    viewdata = {}
    tag = Tags.objects.get(id=request.POST['tagId'])
    tag.delete()
    viewdata["success"] = True
    return JsonResponse(viewdata)


def download_campaigns(request, profile_id):
    page_no = request.POST['page1']
    per_page = request.POST['per_page1']
    order = request.POST['order1']
    order_by = request.POST['order_by1']
    startdate = request.POST['startDate1']
    enddate = request.POST['endDate1']
    filters = request.POST['filters1']
    choosed_columns = json.loads(request.POST['choosed_columns1'])
    choosed_columns = choosed_columns.values()
    query = prepareQuery(startdate, enddate, profile_id, page_no, per_page, order, order_by, filters, 1)
    data_campaigns = Campaigns.objects.raw(query)
    output = io.BytesIO()
    wb = Workbook(output)
    ws = wb.add_worksheet('Report')
    # Sheet header, first row
    bold = wb.add_format({'bold': True})
    row_num = 0
    columns = choosed_columns
    rows = data_campaigns
    rows = list(rows)
    ws.write_row(0, 0, data=columns)
    # Sheet values
    for row in itertools.chain(rows):
        row_num += 1
        data = []
        for colu in json.loads(request.POST['choosed_columns1']).keys():
            data.append(getattr(row, colu))
        ws.write_row(row_num, 0, data=data)

    wb.close()
    output.seek(0)
    filename = 'campaigns_{}_{}.xlsx'.format(profile_id, random.randint(2345678909800, 9923456789000))
    response = HttpResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    response['Content-Disposition'] = 'attachment; filename=%s' % filename
    return response


def get_tags_for_view(request, profile_id):
    viewdata = {}
    campaignIds = request.POST["entityIds"].rstrip(",")
    inner_query = parepare_inner_query(request.POST['limit'])
    query = "SELECT t1.id as id, entity_id, GROUP_CONCAT(tag_name) AS tag_names FROM ( \
            SELECT id, SUBSTRING_INDEX(SUBSTRING_INDEX(entity_ids, ',', numbers.n), ',', -1) AS entity_id\
            FROM ( \
            {} \
            ) numbers \
            JOIN tags ON CHAR_LENGTH(entity_ids) - CHAR_LENGTH(REPLACE(entity_ids, ',', '')) >= numbers.n - 1 \
            ) t1 \
            JOIN tags t2 ON t1.id = t2.id \
            WHERE profile_id = {} AND entity_id IN ({}) GROUP BY entity_id".format(inner_query, profile_id, campaignIds)
    tags = Tags.objects.raw(query)
    tags_list = [
        {'id': tag.id, 'entity_id': tag.entity_id, 'tag_names': tag.tag_names}
        for tag in tags
    ]
    viewdata["tags"] = tags_list
    return JsonResponse(viewdata)


def parepare_inner_query(limit):
    query = "SELECT 1 AS n"
    for i in range(2, int(limit) + 1):
        query += " UNION ALL SELECT {} ".format(i)
    return query


def get_campaign_placements(request, profile_id):
    viewdata = {}
    data = {}
    startdate = request.POST['startDate']
    enddate = request.POST['endDate']
    campaignId = request.POST["campaignId"]
    query = """
        SELECT
            id,
            report_date,
            Round(IFNULL((SUM(cost)/SUM(sales7d))*100,0),2) AS acos,
            placementClassification,
            IFNULL(SUM(impressions), 0) AS impressions,
            IFNULL(SUM(clicks),0) AS clicks,
            IFNULL(SUM(cost),0) AS cost,
            IF(SUM(impressions) = 0, NULL, round(COALESCE(CAST(SUM(clicks) AS DOUBLE)/SUM(impressions), 0)*100,2)) AS ctr,
            IF(SUM(clicks) = 0, NULL, round(COALESCE(CAST(SUM(cost) AS DOUBLE)/SUM(clicks), 0),2)) AS avgCpc,
            IF(SUM(clicks) = 0, NULL, round(COALESCE(CAST(SUM(purchases30d) AS DOUBLE)/SUM(clicks), 0)*100,2)) AS CR,
            IFNULL(SUM(purchases7d), 0) AS purchases7d,
            IFNULL(SUM(sales7d), 0) AS sales7d,
            IFNULL(SUM(unitsSoldClicks7d), 0) AS unitsSoldClicks7d,
            IFNULL(SUM(attributedSalesSameSku7d), 0) AS attributedSalesSameSku7d,
            IFNULL(SUM(unitsSoldSameSku7d), 0) AS unitsSoldSameSku7d
        FROM
            campaign_placements
        WHERE
            campaignId = {}
            AND report_date >= '{}'
            AND report_date <= '{}'
        GROUP BY
            placementClassification
    """.format(
        campaignId,
        datetime.datetime.strptime(startdate, "%Y%m%d").strftime("%Y-%m-%d"),
        datetime.datetime.strptime(enddate, "%Y%m%d").strftime("%Y-%m-%d")
    )

    campaigns_placement = CampaignPlacements.objects.raw(query)

    campaigns_placement_list = [
        {
            'id': placement.id,
            'report_date': placement.report_date,
            'acos': placement.acos,
            'placementClassification': placement.placementClassification,
            'impressions': placement.impressions,
            'clicks': placement.clicks,
            'cost': placement.cost,
            'ctr': placement.ctr,
            'avgCpc': placement.avgCpc,
            'CR': placement.CR,
            'purchases7d': placement.purchases7d,
            'sales7d': placement.sales7d,
            'unitsSoldClicks7d': placement.unitsSoldClicks7d,
            'attributedSalesSameSku7d': placement.attributedSalesSameSku7d,
            'unitsSoldSameSku7d': placement.unitsSoldSameSku7d
        }
        for placement in campaigns_placement
    ]

    viewdata["columns"] = request.POST['choosed_columns']
    viewdata["campaignPlacements"] = campaigns_placement_list
    return JsonResponse(viewdata, safe=False)






def requestActions(request, profile_id):
    action = request.POST['ajaxAction']
    if (action == "fetch_data"):
        return ViewData(request, profile_id)

    if (action == "bulkBidChange"):
        return bidChange(request, profile_id)

    if (action == "bulkStateChange"):
        return stateChange(request, profile_id)

    if (action == "bulkStrategyChange"):
        return strategyChange(request, profile_id)

    if (action == "bulkBiddingPlacementChange"):
        return biddingPlacementChange(request, profile_id)

    if (action == "create_new_tag"):
        return create_new_tag_and_add_campaigns(request, profile_id)

    if (action == "addCampaignsTags"):
        return add_campaigns_to_tag(request, profile_id)

    if (action == "delete_tag"):
        return delete_tag(request, profile_id)

    if (action == "get_tags_for_view"):
        return get_tags_for_view(request, profile_id)

    if (action == "fetch_placement_data"):
        return get_campaign_placements(request, profile_id)

