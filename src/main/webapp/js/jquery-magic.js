
$(document).ready(function(){	

	var build_objects_hash = function (json) {
	  objects = {};
	  
  	$.each(json, function(i, val) {
  	  objects[val.name] = val.schema; 
  	});
  	
  	return objects;
	}
	
  var set_panel_heights = function () {
    var height = $(window).height();
    $("#resultsPane").css("height", (height - 40));
    $("#table").css("height", (height - 40));
  }
  
  var build_eventType_table = function() {
    $.each(objects, function(eventType, schema){
      $('table#eventTypes').append(
        $("<tr>").append(
          $("<td>").text(eventType).attr('name', eventType))
      );					
    });
  }
  
  objects = build_objects_hash(json);
  set_panel_heights();
  build_eventType_table();

	$('table#eventTypes tbody tr').click(function(){

		  eventType = $('td', this).attr('name');
      
      presentation_stuff = function(tr){
  			$("#resultsPane")
  			  .children().remove(".element");

  			$("#resultsPane #title #name")
  			  .html("&nbsp;" + eventType);
  			  
  		  $(tr)
  		    .addClass("selected")
  		    .siblings().removeClass("selected");
      }
      
      create_fields = function(fields){
        $.each(fields, function(index, field_obj){

          element = elementDiv(field_obj);
          e = get_element_std_attributes(element);

  			  element.appendTo($("#resultsPane"));
          return_to_std_mode(element, e);
          $(".navBar .buttons", element).hide();
          
  			});
      }
        
      presentation_stuff(this);
      create_fields(objects[eventType]);
      	

      $(".element").hover(function(){
        $(".navBar .buttons", this).show();
      }, function(){
        $(".navBar .buttons", this).hide();
      });
      
      $(".navBar .remove").click(function(){
        if(confirm("Are you sure you want to deprecate this type?")){
          $(this).parent().parent().parent().slideUp(200, function() {$(this).remove();})
        }
      })
      
      $(".navBar .blue").click(function(){
        var element = $(this).closest(".element");
        var e = get_element_std_attributes(element);
      
        if (e.edit_mode == ""){
          enter_edit_mode(element, e);
        }
      });
      
      $(".details .actions .save").click(function(){
      
        // get element and element attributes
        element = $(this).closest(".element");
        e = get_element_edit_attributes(element);
      
        // get and update field object
        index  = $("#resultsPane .element").index(element);
        object = objects[eventType][index];
        object = save_to_json(element, e, object);
      
        return_to_std_mode(element, e);
      });
      
      $(".details .actions .cancel").click(function(){
            

      
        // get element
        element = $(this).closest(".element");
      
        // get and update field object
        index  = $("#resultsPane .element").index(element);
        object = objects[eventType][index];
      
        return_to_std_mode(element, object);
      
      });
    
      $(".sql .dropdown").change(function(){
        element = $(this).closest(".element");
        sql_param(element, "");
      });
	});		
});



function elementDiv(element) {
  return $("<div>")
    .addClass("element")
    .attr("_edit", "")
  
     .append($("<div>").addClass("navBar")
       .append($("<div>").addClass("name").text(element.name || ""))
       .append($("<div>").addClass("buttons")
         .append($("<div>").addClass("blue"))
         .append($("<div>").addClass("remove"))
        )
       .append($("<div style=\"clear:both\"></div>"))
     )
  
     .append($("<div>").addClass("details")
	     .append($("<div>").addClass("description").html(element.description || ""))
       .append($("<div>").addClass("sql")
         .append($("<ul>").addClass("list")
            .append($("<li>").html("sql: "))
            .append($("<li>").addClass("dropdown").html(element.sql_type || ""))
            .append($("<li>").addClass("param").html(element.sql_param || ""))
         )
       )
	     .append($("<div><ul>").addClass("actions")
	       .append($("<li>").addClass("save").text("save"))
	       .append($("<li>").addClass("cancel").text("cancel"))
	     )
     )
     
//     .append($("<div>").addClass("footer")
//       .append($("<div>").addClass("type").text("type: " + (element.type || "")))
//       .append($("<div>").addClass("position").text("position: " + (element.position || "")))
//     )
     
  .append($("<div style=\"clear:both\"></div>"));
}

function get_element_std_attributes(element){
  return {
    name        : $(".name", element).html(),
    description : $(".description", element).html(),
    sql_type    : $(".sql .dropdown", element).html(),
    sql_param   : $(".sql .param", element).html(),
    edit_mode   : $(element).attr("_edit") || "", 
  }
}

function get_element_edit_attributes(element){

  return {
    name        : $(".name input", element).val(),
    description : $(".description textarea", element).val(),
    sql_type    : $(".sql .dropdown select option:selected", element).text(),
    sql_param   : $(".sql .param input", element).val(),
    edit_mode   : $(element).attr("_edit") || "", 
  }
}

function enter_edit_mode(element, attr){


  // FIX element attributes
  $(".navBar .name", element)
    .addClass("edit")
    .html(
      $("<input>")
      .val(attr.name)
    );

  $(".details .description", element)
    .addClass("edit")
    .html(
      $("<textarea>")
      .val(attr.description)
    );

  $(".details .sql .dropdown", element)
    .html(
      dropdown(attr.sql_type)
    );
  
  sql_param(element, attr.sql_param)
  
  
  // SHOW actions and details pane
  $(".details", element).show();
  $(".details .actions", element).show();
  $(".navBar .buttons", element).show();
  $(element).attr("_edit", "edit");
  
}

function return_to_std_mode(element, attr){

  // FIX element attributes
  $(".name", element)
    .removeClass("edit")
    .html(attr.name);
  
  $(".description", element)
    .removeClass("edit")
    .html(attr.description || "");
  
  $(".sql .dropdown", element)
    .html(attr.sql_type || "");
  
  $(".sql .param", element)
    .html(attr.sql_param || "");
  
  
  // HIDE actions and details pane
  if($(".description", element).html() == ""  && ($(".sql .dropdown", element).text() == "type" || $(".sql .dropdown", element).text() == "")){
    $(".details", element).hide();
  }
  
  $(".details .actions", element).hide();
  $(element).attr("_edit", "");
  
}

function save_to_json(element, attr, object){

  object.name = attr.name;       
  object.description = attr.description;
  object.sql_type = attr.sql_type;
  object.sql_param = attr.sql_param;
  
  return object;
}

function dropdown(sql_type) {
  
  var dropdown = $("<select>");
	var types = ["type", "string", "bool", "byte", "i16", "i32", "i64", "double", "date", "ip"];
  
  build_dropdown = function(dropdown, types) {
    $.each(types, function(index, type){

  		var option = $("<option>").val(type).text(type);

  		if (type == sql_type){
  		  option.attr("selected", "selected");
  		}

  		$(dropdown).append(option);
  	});
  	
    return dropdown;
  }
  
  return build_dropdown(dropdown, types);
}

function sql_param(element, param){

  sql = $(".details .sql", element);
  option = $(".dropdown select option:selected", sql).text();

  if(option == "string") {
    $(".param", sql)
      .html(
        $("<input>").val(param)
      );
  } else {
    $(".param", sql)
      .html("");
  }
}

$(window).resize(function() {
  var height = $(window).height();	
	$("#resultsPane").css("height", (height - 40));
	$("#table").css("height", (height - 40));
});




